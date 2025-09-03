# from pyspark.sql import SparkSession
# from pyspark.sql.functions import year, month, avg, min, max

# spark = SparkSession.builder \
#     .appName("Weather Aggregation") \
#     .config("spark.jars", "/opt/bitnami/spark/user-jars/postgresql-42.7.3.jar") \
#     .getOrCreate()

# jdbc_url = "jdbc:postgresql://postgres:5432/weather"
# connection_properties = {
#     "user": "airflow",
#     "password": "airflow",
#     "driver": "org.postgresql.Driver"
# }

# # Read data
# df = spark.read.jdbc(
#     url=jdbc_url,
#     table="weather_raw",
#     properties=connection_properties
# )

# # Parse year/month from date
# df = df.withColumn("year", year("date"))
# df = df.withColumn("month", month("date"))

# # Aggregations
# agg_df = df.groupBy("year", "month").agg(
#     avg("temperature_min").alias("avg_temp_min"),
#     avg("temperature_max").alias("avg_temp_max"),
#     min("temperature_min").alias("min_temp"),
#     max("temperature_max").alias("max_temp"),
#     avg("precipitation").alias("avg_precip"),
#     min("precipitation").alias("min_precip"),
#     max("precipitation").alias("max_precip")
# )

# # Save result
# agg_df.write.jdbc(
#     url=jdbc_url,
#     table="weather_summary",
#     mode="overwrite",
#     properties=connection_properties
# )

# print("✅ Aggregation complete.")
# spark.stop()

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import year as year_fn, month, avg, min, max

# === Аргументы ===
parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, required=True)
args = parser.parse_args()
target_year = args.year

# === Spark сессия ===
spark = SparkSession.builder \
    .appName(f"Weather Aggregation {target_year}") \
    .config("spark.jars", "/opt/bitnami/spark/user-jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# === Подключение к PostgreSQL ===
jdbc_url = "jdbc:postgresql://postgres:5432/weather"
connection_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# === Загрузка данных ===
df = spark.read.jdbc(
    url=jdbc_url,
    table="weather_raw",
    properties=connection_properties
)

# === Фильтрация по году ===
df = df.withColumn("year", year_fn("date"))
df = df.withColumn("month", month("date"))
df = df.filter(df["year"] == target_year)

# === Агрегации ===
agg_df = df.groupBy("year", "month").agg(
    avg("temperature_min").alias("avg_temp_min"),
    avg("temperature_max").alias("avg_temp_max"),
    min("temperature_min").alias("min_temp"),
    max("temperature_max").alias("max_temp"),
    avg("precipitation").alias("avg_precip"),
    min("precipitation").alias("min_precip"),
    max("precipitation").alias("max_precip")
)

# === Сохранение в таблицу по году ===
agg_df.write.jdbc(
    url=jdbc_url,
    table=f"weather_meta_{target_year}",
    mode="overwrite",
    properties=connection_properties
)

print(f"✅ Aggregation for {target_year} complete.")
spark.stop()
