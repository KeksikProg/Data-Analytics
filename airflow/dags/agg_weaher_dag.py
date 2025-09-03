from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


YEARS = list(range(2020, 2025)) # Указанные года 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='agg_weather_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    meta_tasks = []

    for year in YEARS:
        task = BashOperator(
            task_id=f'aggregate_year_{year}',
            bash_command=f"""
            docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              --executor-memory 1g \
              --name weather-meta-{year} \
              --jars /opt/bitnami/spark/user-jars/postgresql-42.7.3.jar \
              --conf spark.driver.extraClassPath=/opt/bitnami/spark/user-jars/postgresql-42.7.3.jar \
              --conf spark.executor.extraClassPath=/opt/bitnami/spark/user-jars/postgresql-42.7.3.jar \
              /opt/bitnami/spark/user-scripts/spark_job.py --year {year}
            """,
        )
        meta_tasks.append(task)

    # Финальный таск после всех
    overall_stats = BashOperator(
        task_id='overall_stats',
        bash_command="""
        docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --executor-memory 1g \
          --name overall-weather \
          --jars /opt/bitnami/spark/user-jars/postgresql-42.7.3.jar \
          --conf spark.driver.extraClassPath=/opt/bitnami/spark/user-jars/postgresql-42.7.3.jar \
          --conf spark.executor.extraClassPath=/opt/bitnami/spark/user-jars/postgresql-42.7.3.jar \
          /opt/bitnami/spark/user-scripts/overall_stats_job.py
        """
    )

    for task in meta_tasks:
        task >> overall_stats
