from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Overall Weather Stats") \
    .config("spark.jars", "/opt/bitnami/spark/user-jars/postgresql-42.7.3.jar") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/weather"
props = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

# Читаем все таблицы меты по годам
dfs = []
for year in range(2020, 2025):
    df = spark.read.jdbc(url=jdbc_url, table=f"weather_meta_{year}", properties=props)
    dfs.append(df)

# Объединяем все таблицы
full_df = dfs[0]
for df in dfs[1:]:
    full_df = full_df.unionByName(df)

# Агрегация общей статистики
summary_df = full_df.groupBy("month").agg({
    "avg_temp_min": "avg",
    "avg_temp_max": "avg",
    "avg_precip": "avg"
}).withColumnRenamed("avg(avg_temp_min)", "avg_temp_min") \
 .withColumnRenamed("avg(avg_temp_max)", "avg_temp_max") \
 .withColumnRenamed("avg(avg_precip)", "avg_precip")

summary_df.write.jdbc(url=jdbc_url, table="weather_overall", mode="overwrite", properties=props)
print("✅ Overall aggregation complete.")
spark.stop()
