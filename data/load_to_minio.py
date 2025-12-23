import os
from pathlib import Path

from minio import Minio
from minio.error import S3Error


# Имена переменных с префиксом AKV_, чтобы не конфликтовать с существующими кластерами
MINIO_ENDPOINT = os.getenv("AKV_MINIO_ENDPOINT", os.getenv("MINIO_ENDPOINT", "localhost:29000"))
MINIO_ACCESS_KEY = os.getenv("AKV_MINIO_ACCESS_KEY", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("AKV_MINIO_SECRET_KEY", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
RAW_BUCKET = os.getenv("AKV_RAW_BUCKET", "akv-itsm-raw")
DM_BUCKET = os.getenv("AKV_DM_BUCKET", "akv-itsm-dm")  # создаем заранее, чтобы Spark мог писать dm
RAW_PREFIX = os.getenv("AKV_RAW_PREFIX", "2024/11")


def upload_raw() -> None:
    base_dir = Path(__file__).resolve().parent
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    for bucket in [RAW_BUCKET, DM_BUCKET]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Создал бакет {bucket}")

    files = ["itsm_incidents.csv", "itsm_requests.csv"]
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
