import io
import os
import sys
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from minio import Minio

sys.path.append("/opt/airflow/data")
from generate_data import generate as generate_raw_data  # noqa: E402
from load_to_minio import upload_raw as upload_raw_to_minio  # noqa: E402


def _fetch_csv(client: Minio, bucket: str, prefix: str, name: str) -> str:
    objects = [
        obj
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True)
        if obj.object_name.endswith(".csv")
    ]
    if not objects:
        raise RuntimeError(f"CSV для {name} не найден в {bucket}/{prefix}")
    csv_obj = client.get_object(bucket, objects[0].object_name)
    return csv_obj.read().decode("utf-8")


def load_all_layers_to_postgres() -> None:
    client = Minio(
        os.getenv("SONYA_WMS_MINIO_ENDPOINT", "sonya-wms-minio:9000"),
        access_key=os.getenv("SONYA_WMS_MINIO_ACCESS_KEY", "sonyawms_admin"),
        secret_key=os.getenv("SONYA_WMS_MINIO_SECRET_KEY", "sonyawms_admin_pwd"),
        secure=False,
    )

    raw_bucket = os.getenv("SONYA_WMS_RAW_BUCKET", "sonya-wms-raw")
    raw_prefix = os.getenv("SONYA_WMS_RAW_PREFIX", "2024/12")
    dds_bucket = os.getenv("SONYA_WMS_DDS_BUCKET", "sonya-wms-dds")
    dds_prefix = os.getenv("SONYA_WMS_DDS_PREFIX", "2024/12/dds")
    dm_bucket = os.getenv("SONYA_WMS_DM_BUCKET", "sonya-wms-dm")
    dm_prefix = os.getenv("SONYA_WMS_DM_PREFIX", "2024/12/dm")

    conn = psycopg2.connect(
        host=os.getenv("SONYA_WMS_POSTGRES_HOST", "sonya-wms-postgres"),
        dbname=os.getenv("SONYA_WMS_POSTGRES_DB", "sonya_wms_dwh"),
        user=os.getenv("SONYA_WMS_POSTGRES_USER", "sonya_wms_user"),
        password=os.getenv("SONYA_WMS_POSTGRES_PASSWORD", "sonya_wms_pwd123"),
    )
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS dds_stg")
    cur.execute("CREATE SCHEMA IF NOT EXISTS dds")
    cur.execute("CREATE SCHEMA IF NOT EXISTS dm")
    conn.commit()

    stage_tables = {
        "equipment": """
            equipment_id TEXT,
            inventory_number TEXT,
            name TEXT,
            category TEXT,
            model TEXT,
            serial_number TEXT,
            status TEXT,
            location_id INT,
            purchase_date DATE,
            warranty_end_date DATE,
            last_maintenance_date DATE,
            next_maintenance_date DATE,
            responsible_person_id INT,
            created_at TIMESTAMP
        """,
        "storage_locations": """
            location_id INT,
            zone TEXT,
            rack TEXT,
            shelf TEXT,
            is_active BOOLEAN
        """,
        "employees": """
            responsible_person_id INT,
            full_name TEXT,
            role TEXT,
            department TEXT,
            contact TEXT,
            shift TEXT
        """,
        "inventory_sessions": """
            inventory_id TEXT,
            session_date DATE,
            location_id INT,
            responsible_person_id INT,
            status TEXT,
            discrepancy_count INT,
            notes TEXT
        """,
        "inventory_results": """
            result_id TEXT,
            inventory_id TEXT,
            equipment_id TEXT,
            location_id INT,
            expected_presence INT,
            actual_presence INT,
            condition TEXT,
            discrepancy_flag BOOLEAN,
            discrepancy_type TEXT,
            comment TEXT
        """,
        "maintenance_logs": """
            maintenance_id TEXT,
            equipment_id TEXT,
            maintenance_date DATE,
            maintenance_type TEXT,
            duration_hours DOUBLE PRECISION,
            status TEXT,
            responsible_person_id INT,
            cost DOUBLE PRECISION
        """,
    }

    dm_tables = {
        "equipment_health": """
            responsible_person_id INT,
            location_id INT,
            equipment_id TEXT,
            inventory_number TEXT,
            name TEXT,
            category TEXT,
            model TEXT,
            serial_number TEXT,
            status TEXT,
            purchase_date DATE,
            warranty_end_date DATE,
            last_maintenance_date DATE,
            next_maintenance_date DATE,
            created_at TIMESTAMP,
            zone TEXT,
            rack TEXT,
            shelf TEXT,
            is_active BOOLEAN,
            full_name TEXT,
            department TEXT,
            shift TEXT,
            age_days INT,
            age_years DOUBLE PRECISION,
            warranty_days_left INT,
            days_since_last_maintenance INT,
            days_to_next_maintenance INT,
            maintenance_overdue BOOLEAN
        """,
        "inventory_accuracy": """
            inventory_id TEXT,
            session_date DATE,
            zone TEXT,
            rack TEXT,
            shelf TEXT,
            checked_equipment BIGINT,
            discrepancies BIGINT,
            actual_presence_sum BIGINT,
            inventory_accuracy DOUBLE PRECISION
        """,
        "location_utilization": """
            zone TEXT,
            rack TEXT,
            shelf TEXT,
            is_active BOOLEAN,
            equipment_count BIGINT,
            working BIGINT,
            in_repair BIGINT,
            in_reserve BIGINT
        """,
        "maintenance_summary": """
            equipment_id TEXT,
            category TEXT,
            maintenance_events BIGINT,
            overdue_events BIGINT,
            total_duration_hours DOUBLE PRECISION,
            total_cost DOUBLE PRECISION,
            last_maintenance_date DATE
        """,
        "responsible_load": """
            responsible_person_id INT,
            assigned_equipment BIGINT,
            working BIGINT,
            in_repair BIGINT,
            full_name TEXT,
            role TEXT,
            department TEXT,
            contact TEXT,
            shift TEXT
        """,
    }

    def load_from_minio(bucket: str, schema: str, table: str, columns_sql: str, csv_prefix: str) -> None:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns_sql})")
        cur.execute(f"TRUNCATE TABLE {schema}.{table}")
        csv_text = _fetch_csv(client, bucket, prefix=csv_prefix, name=table)
        cur.copy_expert(
            f"COPY {schema}.{table} FROM STDIN WITH CSV HEADER",
            io.StringIO(csv_text),
        )
        conn.commit()
        print(f"✅ Загружена таблица {schema}.{table} из {bucket}/{csv_prefix}")

    for name, columns_sql in stage_tables.items():
        load_from_minio(raw_bucket, "dds_stg", name, columns_sql, f"{raw_prefix}/{name}.csv")

    for name, columns_sql in stage_tables.items():
        load_from_minio(dds_bucket, "dds", name, columns_sql, f"{dds_prefix}/{name}_csv")

    for name, columns_sql in dm_tables.items():
        load_from_minio(dm_bucket, "dm", name, columns_sql, f"{dm_prefix}/{name}_csv")

    cur.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sonya_wms_inventory_pipeline",
    start_date=datetime(2024, 12, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["wms", "inventory", "babaevsky"],
) as dag:

    generate_raw = PythonOperator(
        task_id="generate_raw",
        python_callable=generate_raw_data,
    )

    upload_raw = PythonOperator(
        task_id="upload_raw_to_minio",
        python_callable=upload_raw_to_minio,
    )

    run_spark = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/airflow/dags/spark_job.py",
        conn_id="spark_default",
        verbose=True,
        conf={"spark.master": "spark://sonya-wms-spark-master:7077"},
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
    )

    load_layers = PythonOperator(
        task_id="load_layers_to_postgres",
        python_callable=load_all_layers_to_postgres,
    )

    generate_raw >> upload_raw >> run_spark >> load_layers
