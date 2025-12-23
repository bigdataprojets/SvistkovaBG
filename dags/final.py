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


def load_dm_to_postgres() -> None:
    client = Minio(
        os.getenv("AKV_MINIO_ENDPOINT", "akv-minio:9000"),
        access_key=os.getenv("AKV_MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("AKV_MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )

    dm_bucket = os.getenv("AKV_DM_BUCKET", "akv-itsm-dm")
    dm_prefix = os.getenv("AKV_DM_PREFIX", "2024/11")

    conn = psycopg2.connect(
        host=os.getenv("AKV_POSTGRES_HOST", "akv-postgres"),
        dbname=os.getenv("AKV_POSTGRES_DB", "akv_itsm"),
        user=os.getenv("AKV_POSTGRES_USER", "akv_user"),
        password=os.getenv("AKV_POSTGRES_PASSWORD", "akv_pwd123"),
    )
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dm_incident_kpis (
            service TEXT,
            priority TEXT,
            opened_date DATE,
            opened INTEGER,
            resolved INTEGER,
            sla_breaches INTEGER,
            mttr_minutes DOUBLE PRECISION,
            sla_compliance_pct DOUBLE PRECISION,
            backlog INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dm_request_kpis (
            service TEXT,
            request_type TEXT,
            date DATE,
            requested INTEGER,
            completed INTEGER,
            avg_fulfillment_hours DOUBLE PRECISION
        )
        """
    )
    conn.commit()

    targets = {
        "incident_kpis": {
            "table": "dm_incident_kpis",
            "columns": [
                "service",
                "priority",
                "opened_date",
                "opened",
                "resolved",
                "sla_breaches",
                "mttr_minutes",
                "sla_compliance_pct",
                "backlog",
            ],
        },
        "request_kpis": {
            "table": "dm_request_kpis",
            "columns": [
                "service",
                "request_type",
                "date",
                "requested",
                "completed",
                "avg_fulfillment_hours",
            ],
        },
    }

    for name, meta in targets.items():
        prefix = f"{dm_prefix}/{name}_csv"
        objects = [
            obj
            for obj in client.list_objects(dm_bucket, prefix=prefix, recursive=True)
            if obj.object_name.endswith(".csv")
        ]
        if not objects:
            print(f"⚠️ CSV для {name} не найден в {dm_bucket}/{prefix}")
            continue

        csv_obj = client.get_object(dm_bucket, objects[0].object_name)
        csv_text = csv_obj.read().decode("utf-8")

        cur.execute(f"TRUNCATE TABLE {meta['table']}")
        cur.copy_expert(
            f"COPY {meta['table']} ({', '.join(meta['columns'])}) FROM STDIN WITH CSV HEADER",
            io.StringIO(csv_text),
        )
        conn.commit()
        print(f"✅ DM {name} загружен в таблицу {meta['table']}")

    cur.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="itsm_daily_pipeline",
    start_date=datetime(2024, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["itsm", "akvalife"],
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
        conf={"spark.master": "spark://akv-spark-master:7077"},
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
    )

    load_dm = PythonOperator(
        task_id="load_dm_to_postgres",
        python_callable=load_dm_to_postgres,
    )

    generate_raw >> upload_raw >> run_spark >> load_dm
