from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="main_weather_pipeline",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    trigger_fetch = TriggerDagRunOperator(
        task_id="download_weather_dag",
        trigger_dag_id="download_weather_dag",
    )

    trigger_yearly = TriggerDagRunOperator(
        task_id="load_database_dag",
        trigger_dag_id="load_database_dag",
        wait_for_completion=True,
    )

    trigger_aggregate = TriggerDagRunOperator(
        task_id="agg_weather_dag",
        trigger_dag_id="agg_weather_dag",
        wait_for_completion=True,
    )

    trigger_fetch >> trigger_yearly >> trigger_aggregate
