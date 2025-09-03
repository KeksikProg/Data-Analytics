from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Export Weather Overall") \
    .config("spark.jars", "/opt/bitnami/spark/user-jars/postgresql-42.7.3.jar") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/weather"
connection_props = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Чтение
df = spark.read.jdbc(jdbc_url, "weather_overall", properties=connection_props)

print(f"🔎 Строк: {df.count()}")
df.show()

# Сохранение как один CSV-файл с заголовками
df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/opt/bitnami/spark/output/weather_overall")

print("✅ CSV export done")
spark.stop()
