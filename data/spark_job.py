import os

from pyspark.sql import SparkSession, functions as F


RAW_BUCKET = os.getenv("SONYA_WMS_RAW_BUCKET", "sonya-wms-raw")
RAW_PREFIX = os.getenv("SONYA_WMS_RAW_PREFIX", "2024/12")
DDS_BUCKET = os.getenv("SONYA_WMS_DDS_BUCKET", "sonya-wms-dds")
DDS_PREFIX = os.getenv("SONYA_WMS_DDS_PREFIX", "2024/12/dds")
DM_BUCKET = os.getenv("SONYA_WMS_DM_BUCKET", "sonya-wms-dm")
DM_PREFIX = os.getenv("SONYA_WMS_DM_PREFIX", "2024/12/dm")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Sonya WMS inventory pipeline")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("SONYA_WMS_MINIO_ENDPOINT", "http://sonya-wms-minio:9000"),
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("SONYA_WMS_MINIO_ACCESS_KEY", "sonyawms_admin"))
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("SONYA_WMS_MINIO_SECRET_KEY", "sonyawms_admin_pwd"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def write_dataset(df, bucket: str, prefix: str, name: str) -> None:
    parquet_path = f"s3a://{bucket}/{prefix}/{name}"
    csv_path = f"{parquet_path}_csv"
    df.write.mode("overwrite").parquet(parquet_path)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
    print(f"✅ {name} сохранен в {parquet_path} и {csv_path}")


def load_raw_tables(spark: SparkSession):
    base_path = f"s3a://{RAW_BUCKET}/{RAW_PREFIX}"
    equipment = (
        spark.read.csv(f"{base_path}/equipment.csv", header=True, inferSchema=True)
        .withColumn("purchase_date", F.to_date("purchase_date"))
        .withColumn("warranty_end_date", F.to_date("warranty_end_date"))
        .withColumn("last_maintenance_date", F.to_date("last_maintenance_date"))
        .withColumn("next_maintenance_date", F.to_date("next_maintenance_date"))
        .withColumn("location_id", F.col("location_id").cast("int"))
        .withColumn("responsible_person_id", F.col("responsible_person_id").cast("int"))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )

    storage_locations = (
        spark.read.csv(f"{base_path}/storage_locations.csv", header=True, inferSchema=True)
        .withColumn("location_id", F.col("location_id").cast("int"))
        .withColumn("is_active", F.col("is_active").cast("boolean"))
    )

    employees = (
        spark.read.csv(f"{base_path}/employees.csv", header=True, inferSchema=True)
        .withColumn("responsible_person_id", F.col("responsible_person_id").cast("int"))
    )

    inventory_sessions = (
        spark.read.csv(f"{base_path}/inventory_sessions.csv", header=True, inferSchema=True)
        .withColumn("session_date", F.to_date("session_date"))
        .withColumn("location_id", F.col("location_id").cast("int"))
        .withColumn("responsible_person_id", F.col("responsible_person_id").cast("int"))
        .withColumn("discrepancy_count", F.col("discrepancy_count").cast("int"))
    )

    inventory_results = (
        spark.read.csv(f"{base_path}/inventory_results.csv", header=True, inferSchema=True)
        .withColumn("location_id", F.col("location_id").cast("int"))
        .withColumn("expected_presence", F.col("expected_presence").cast("int"))
        .withColumn("actual_presence", F.col("actual_presence").cast("int"))
        .withColumn("discrepancy_flag", F.col("discrepancy_flag").cast("boolean"))
    )

    maintenance_logs = (
        spark.read.csv(f"{base_path}/maintenance_logs.csv", header=True, inferSchema=True)
        .withColumn("maintenance_date", F.to_date("maintenance_date"))
        .withColumn("duration_hours", F.col("duration_hours").cast("double"))
        .withColumn("cost", F.col("cost").cast("double"))
        .withColumn("responsible_person_id", F.col("responsible_person_id").cast("int"))
        .withColumnRenamed("status", "maintenance_status")
    )

    return {
        "equipment": equipment,
        "storage_locations": storage_locations,
        "employees": employees,
        "inventory_sessions": inventory_sessions,
        "inventory_results": inventory_results,
        "maintenance_logs": maintenance_logs,
    }


def build_dds(raw_tables: dict) -> None:
    for name, df in raw_tables.items():
        write_dataset(df, DDS_BUCKET, DDS_PREFIX, name)


def build_dm(raw_tables: dict) -> None:
    equipment = raw_tables["equipment"]
    storage_locations = raw_tables["storage_locations"]
    employees = raw_tables["employees"]
    inventory_sessions = raw_tables["inventory_sessions"]
    inventory_results = raw_tables["inventory_results"]
    maintenance_logs = raw_tables["maintenance_logs"]

    today = F.current_date()

    equipment_health = (
        equipment.join(storage_locations, on="location_id", how="left")
        .join(
            employees.select("responsible_person_id", "full_name", "department", "shift"),
            on="responsible_person_id",
            how="left",
        )
        .withColumn("age_days", F.datediff(today, "purchase_date"))
        .withColumn("age_years", F.round(F.col("age_days") / 365.0, 2))
        .withColumn("warranty_days_left", F.datediff("warranty_end_date", today))
        .withColumn("days_since_last_maintenance", F.datediff(today, "last_maintenance_date"))
        .withColumn("days_to_next_maintenance", F.datediff("next_maintenance_date", today))
        .withColumn("maintenance_overdue", F.col("next_maintenance_date") < today)
    )
    write_dataset(equipment_health, DM_BUCKET, DM_PREFIX, "equipment_health")

    inventory_accuracy = (
        inventory_results.join(inventory_sessions, on="inventory_id", how="left")
        .join(storage_locations, on="location_id", how="left")
        .groupBy("inventory_id", "session_date", "zone", "rack", "shelf")
        .agg(
            F.count("*").alias("checked_equipment"),
            F.sum(F.when(F.col("discrepancy_flag"), 1).otherwise(0)).alias("discrepancies"),
            F.sum("actual_presence").alias("actual_presence_sum"),
        )
        .withColumn(
            "inventory_accuracy",
            F.when(F.col("checked_equipment") > 0, 1 - (F.col("discrepancies") / F.col("checked_equipment"))),
        )
        .orderBy("session_date")
    )
    write_dataset(inventory_accuracy, DM_BUCKET, DM_PREFIX, "inventory_accuracy")

    location_utilization = (
        equipment.join(storage_locations, on="location_id", how="left")
        .groupBy("zone", "rack", "shelf", "is_active")
        .agg(
            F.count("*").alias("equipment_count"),
            F.sum(F.when(F.col("status") == "рабочее", 1).otherwise(0)).alias("working"),
            F.sum(F.when(F.col("status") == "на ремонте", 1).otherwise(0)).alias("in_repair"),
            F.sum(F.when(F.col("status") == "резерв", 1).otherwise(0)).alias("in_reserve"),
        )
        .orderBy("zone", "rack", "shelf")
    )
    write_dataset(location_utilization, DM_BUCKET, DM_PREFIX, "location_utilization")

    maintenance_summary = (
        maintenance_logs.join(equipment.select("equipment_id", "category"), on="equipment_id", how="left")
        .groupBy("equipment_id", "category")
        .agg(
            F.count("*").alias("maintenance_events"),
            F.sum(F.when(F.col("maintenance_status") == "просрочено", 1).otherwise(0)).alias("overdue_events"),
            F.sum("duration_hours").alias("total_duration_hours"),
            F.sum("cost").alias("total_cost"),
            F.max("maintenance_date").alias("last_maintenance_date"),
        )
    )
    write_dataset(maintenance_summary, DM_BUCKET, DM_PREFIX, "maintenance_summary")

    responsible_load = (
        equipment.groupBy("responsible_person_id")
        .agg(
            F.count("*").alias("assigned_equipment"),
            F.sum(F.when(F.col("status") == "рабочее", 1).otherwise(0)).alias("working"),
            F.sum(F.when(F.col("status") == "на ремонте", 1).otherwise(0)).alias("in_repair"),
        )
        .join(employees, on="responsible_person_id", how="left")
    )
    write_dataset(responsible_load, DM_BUCKET, DM_PREFIX, "responsible_load")


if __name__ == "__main__":
    spark_session = create_spark_session()
    print("=" * 60)
    print("Spark сессия создана с конфигурацией MinIO для WMS")
    print(f"Spark версия: {spark_session.version}")
    print("=" * 60)

    try:
        raw_tables_dict = load_raw_tables(spark_session)
        build_dds(raw_tables_dict)
        build_dm(raw_tables_dict)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"❌ Ошибка при расчете витрин: {exc}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        spark_session.stop()
