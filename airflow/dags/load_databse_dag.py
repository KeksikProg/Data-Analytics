from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import psycopg2

DATA_DIR = '/opt/airflow/data/raw'
PG_CONN = {
    'host': 'postgres',
    'port': 5432,
    'user': 'airflow',
    'password': 'airflow',
    'dbname': 'weather'
}

TABLE_NAME = 'weather_raw'

def create_table_if_not_exists():
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date DATE,
            temperature_min REAL,
            temperature_max REAL,
            precipitation REAL,
            year INT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Таблица создана")

def load_csv_to_postgres(file_name):
    year = int(file_name.split('_')[-1].split('.')[0])
    file_path = os.path.join(DATA_DIR, file_name)
    df = pd.read_csv(file_path)
    df['year'] = year
    df.rename(columns={
        'temperature_2m_min': 'temperature_min',
        'temperature_2m_max': 'temperature_max',
        'precipitation_sum': 'precipitation'
    }, inplace=True)

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    # Проверим, есть ли уже записи за этот год
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE year = %s", (year,))
    count = cur.fetchone()[0]
    if count > 0:
        print(f"Год {year} уже загружен")
        cur.close()
        conn.close()
        return

    # Вставим данные
    for _, row in df.iterrows():
        cur.execute(f"""
            INSERT INTO {TABLE_NAME} (date, temperature_min, temperature_max, precipitation, year)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['time'], row['temperature_min'], row['temperature_max'], row['precipitation'], year))

    conn.commit()
    cur.close()
    conn.close()
    print(f"Загружено {file_name} в таблицу {TABLE_NAME}")

def generate_load_tasks(dag):
    create = PythonOperator(
        task_id='create_table',
        python_callable=create_table_if_not_exists,
        dag=dag,
    )

    tasks = []
    for file_name in sorted(os.listdir(DATA_DIR)):
        if file_name.endswith('.csv') and file_name.startswith('weather_'):
            task = PythonOperator(
                task_id=f'load_{file_name.replace(".csv", "")}',
                python_callable=load_csv_to_postgres,
                op_args=[file_name],
                dag=dag,
            )
            create >> task  # depends on create_table
            tasks.append(task)

    return tasks

with DAG(
    dag_id='load_database_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    load_tasks = generate_load_tasks(dag)
