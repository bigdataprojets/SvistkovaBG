import os
from pathlib import Path

from minio import Minio
from minio.error import S3Error


MINIO_ENDPOINT = os.getenv("SONYA_WMS_MINIO_ENDPOINT", os.getenv("MINIO_ENDPOINT", "localhost:49100"))
MINIO_ACCESS_KEY = os.getenv("SONYA_WMS_MINIO_ACCESS_KEY", os.getenv("MINIO_ACCESS_KEY", "sonyawms_admin"))
MINIO_SECRET_KEY = os.getenv("SONYA_WMS_MINIO_SECRET_KEY", os.getenv("MINIO_SECRET_KEY", "sonyawms_admin_pwd"))
RAW_BUCKET = os.getenv("SONYA_WMS_RAW_BUCKET", "sonya-wms-raw")
DDS_BUCKET = os.getenv("SONYA_WMS_DDS_BUCKET", "sonya-wms-dds")
DM_BUCKET = os.getenv("SONYA_WMS_DM_BUCKET", "sonya-wms-dm")
RAW_PREFIX = os.getenv("SONYA_WMS_RAW_PREFIX", "2024/12")


def upload_raw() -> None:
    base_dir = Path(__file__).resolve().parent
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    for bucket in [RAW_BUCKET, DDS_BUCKET, DM_BUCKET]:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"Создал бакет {bucket}")
        except S3Error as exc:
            if exc.code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                raise
            print(f"Бакет {bucket} уже существует, продолжаю")

    files = [
        "storage_locations.csv",
        "employees.csv",
        "equipment.csv",
        "inventory_sessions.csv",
        "inventory_results.csv",
        "maintenance_logs.csv",
    ]
    for file in files:
        src_path = base_dir / "raw" / file
        if not src_path.exists():
            print(f"WARN: файл {src_path} не найден, пропускаю")
            continue
        object_name = f"{RAW_PREFIX}/{file}"
        client.fput_object(RAW_BUCKET, object_name, src_path.as_posix())
        print(f"Загружен {object_name} в бакет {RAW_BUCKET}")


if __name__ == "__main__":
    try:
        upload_raw()
    except S3Error as exc:
        print(f"Ошибка работы с MinIO: {exc}")
