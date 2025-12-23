from pyspark.sql import SparkSession

print("=" * 60)
print("ТЕСТ: Подключение Spark к MinIO")
print("=" * 60)

try:
    spark = SparkSession.builder \
        .appName("MinIO_Connection_Test") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://sonya-wms-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "sonyawms_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "sonyawms_admin_pwd") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    print(f"✅ Spark session created: {spark.version}")
    
    # Попробуем прочитать список файлов в bucket
    from pyspark.sql import DataFrame
    import traceback
    
    try:
        df = spark.read.csv("s3a://sonya-wms-raw/2024/12/equipment.csv", header=True, inferSchema=True)
        print(f"✅ Успешно прочитано из MinIO: {df.count()} строк")
        df.show(5)
        
    except Exception as e:
        print(f"❌ Ошибка при чтении из MinIO: {e}")
        print("\nДетали ошибки:")
        traceback.print_exc()
        
        # Проверяем, видит ли Spark библиотеку S3AFileSystem
        print("\nПроверка доступности S3AFileSystem:")
        spark._jsc.hadoopConfiguration().get("fs.s3a.impl")
        
    spark.stop()
    
except Exception as e:
    print(f"❌ Ошибка создания Spark сессии: {e}")
    import traceback
    traceback.print_exc()

print("=" * 60)
