from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests
import pandas as pd

DATA_DIR = '/opt/airflow/data/raw'
LAT, LON = 55.75, 37.61  # Москва
YEARS_BACK = 5

def fetch_weather_for_year(year: int, lat: float, lon: float):
    os.makedirs(DATA_DIR, exist_ok=True)
    file_path = os.path.join(DATA_DIR, f'weather_{year}.csv')
    if os.path.exists(file_path):
        print(f"Файл уже есть: {file_path} — пропущено")
        return

    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={year}-01-01&end_date={year}-12-31"
        f"&daily=temperature_2m_min,temperature_2m_max,precipitation_sum"
        f"&timezone=auto"
    )

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data['daily'])
    df.to_csv(file_path, index=False)
    print(f"Сохранено: {file_path}")

def create_download_tasks(dag):
    tasks = []
    current_year = datetime.now().year
    for year in range(current_year - YEARS_BACK, current_year):
        task = PythonOperator(
            task_id=f'download_weather_{year}',
            python_callable=fetch_weather_for_year,
            op_args=[year, LAT, LON],
            dag=dag,
        )
        tasks.append(task)
    return tasks

with DAG(
    dag_id='download_weather_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    download_tasks = create_download_tasks(dag)
