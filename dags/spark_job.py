import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


RAW_BUCKET = os.getenv("AKV_RAW_BUCKET", "akv-itsm-raw")
RAW_PREFIX = os.getenv("AKV_RAW_PREFIX", "2024/11")
DM_BUCKET = os.getenv("AKV_DM_BUCKET", "akv-itsm-dm")
DM_PREFIX = os.getenv("AKV_DM_PREFIX", "2024/11")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ITSM pipeline")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("AKV_MINIO_ENDPOINT", "http://akv-minio:9000"),
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AKV_MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AKV_MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def write_dm(df, name: str) -> None:
    parquet_path = f"s3a://{DM_BUCKET}/{DM_PREFIX}/{name}"
    csv_path = f"{parquet_path}_csv"
    df.write.mode("overwrite").parquet(parquet_path)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
    print(f"✅ DM {name} сохранен в {parquet_path} и {csv_path}")


def build_dm(spark: SparkSession) -> None:
    incidents = (
        spark.read.csv(
            f"s3a://{RAW_BUCKET}/{RAW_PREFIX}/itsm_incidents.csv",
            header=True,
            inferSchema=True,
        )
        .withColumn("opened_at", F.to_timestamp("opened_at"))
        .withColumn("resolved_at", F.to_timestamp("resolved_at"))
        .withColumn("sla_due", F.to_timestamp("sla_due"))
        .withColumn(
            "resolution_minutes",
            F.when(
                F.col("resolved_at").isNotNull(),
                (F.col("resolved_at").cast("long") - F.col("opened_at").cast("long")) / 60,
            ),
        )
        .withColumn("sla_breached", F.col("sla_breached").cast("boolean"))
        .withColumn("opened_date", F.to_date("opened_at"))
        .withColumn("resolved_date", F.to_date("resolved_at"))
    )

    incident_kpis = (
        incidents.groupBy("service", "priority", "opened_date")
        .agg(
            F.count("*").alias("opened"),
            F.count(F.when(F.col("resolved_at").isNotNull(), True)).alias("resolved"),
            F.sum(F.when(F.col("sla_breached") == True, 1).otherwise(0)).alias(
                "sla_breaches"
            ),
            F.avg("resolution_minutes").alias("mttr_minutes"),
        )
        .withColumn(
            "sla_compliance_pct",
            F.when(
                F.col("opened") > 0,
                (1 - (F.col("sla_breaches") / F.col("opened"))) * 100,
            ),
        )
    )

    backlog_window = (
        Window.partitionBy("service", "priority")
        .orderBy("opened_date")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    incident_kpis = (
        incident_kpis.withColumn("opened_cum", F.sum("opened").over(backlog_window))
        .withColumn("resolved_cum", F.sum("resolved").over(backlog_window))
        .withColumn("backlog", F.col("opened_cum") - F.col("resolved_cum"))
        .drop("opened_cum", "resolved_cum")
    )

    requests = (
        spark.read.csv(
            f"s3a://{RAW_BUCKET}/{RAW_PREFIX}/itsm_requests.csv",
            header=True,
            inferSchema=True,
        )
        .withColumn("opened_at", F.to_timestamp("opened_at"))
        .withColumn("closed_at", F.to_timestamp("closed_at"))
        .withColumn("opened_date", F.to_date("opened_at"))
        .withColumn("closed_date", F.to_date("closed_at"))
    )

    request_kpis = (
        requests.groupBy("service", "request_type", "opened_date")
        .agg(
            F.count("*").alias("requested"),
            F.count(F.when(F.col("status") == "Завершен", True)).alias("completed"),
            F.avg("fulfillment_hours").alias("avg_fulfillment_hours"),
        )
        .withColumnRenamed("opened_date", "date")
    )

    write_dm(incident_kpis, "incident_kpis")
    write_dm(request_kpis, "request_kpis")


if __name__ == "__main__":
    spark_session = create_spark_session()
    print("=" * 60)
    print("Spark сессия создана с конфигурацией MinIO")
    print(f"Spark версия: {spark_session.version}")
    print("=" * 60)

    try:
        build_dm(spark_session)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"❌ Ошибка при расчете DM: {exc}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        spark_session.stop()
