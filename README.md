# Data Analytics (Weather Pipeline) 🌦️📊

**Weather Data Pipeline** — учебный проект, реализующий полный ETL-процесс:  
скачивание погодных данных с API, их загрузка в базу данных, агрегация в Spark и визуализация итоговой статистики.  
Оркестрация пайплайна выполнена в **Apache Airflow**, развёртывание — в **Docker**.

---

## ✨ Основные возможности
- Автоматическая загрузка исторических погодных данных (через API Open-Meteo)  
- Сохранение сырых данных в CSV и PostgreSQL  
- Годовые и месячные агрегации с использованием Spark  
- Подсчёт общей статистики за несколько лет  
- Экспорт итогов в CSV  
- Визуализация данных и прогнозов в Jupyter Notebook  

---

## 📂 Структура проекта

- **`download_weather_dag.py`**  
  DAG Airflow для скачивания погодных данных за последние 5 лет.  
  - API: [Open-Meteo Archive](https://open-meteo.com/)  
  - Сохраняет CSV в `data/raw/`

- **`load_database_dag.py`**  
  DAG Airflow для загрузки CSV в PostgreSQL (`weather_raw`).  
  - Создаёт таблицу при отсутствии  
  - Добавляет год к строкам  
  - Пропускает уже загруженные годы  

- **`agg_weaher_dag.py`**  
  DAG Airflow для агрегации данных по каждому году с помощью Spark (`spark_job.py`).  
  - Параллельные задачи по годам  
  - Финальный таск собирает общую статистику (`overall_stats_job.py`)  

- **`main_weather_pipeline.py`**  
  Главный DAG, который связывает все три этапа: download_weather_dag → load_database_dag → agg_weather_dag

- **`spark_job.py`**  
Spark-скрипт для агрегации данных по конкретному году:  
- min/max/avg температуры и осадков  
- сохраняет результат в таблицу `weather_meta_<год>`

- **`overall_stats_job.py`**  
Spark-скрипт для объединения данных всех лет и подсчёта сводной статистики  
(средние по месяцам). Результат сохраняется в `weather_overall`.

- **`export_weather_overall.py`**  
Spark-скрипт для экспорта `weather_overall` в CSV (один файл с заголовками).

- **`weather_analysis.ipynb`**  
Jupyter Notebook для визуализации:  
- графики по месяцам и годам  
- динамика температуры и осадков  
- при желании — прогноз следующего года  

- **`Dockerfile`**  
Docker-образ со всеми зависимостями (Airflow, Spark, PostgreSQL драйверы).

---

## ⚙️ Используемые технологии
- [Apache Airflow](https://airflow.apache.org/) — оркестрация пайплайна  
- [Apache Spark](https://spark.apache.org/) — обработка данных  
- [PostgreSQL](https://www.postgresql.org/) — хранилище данных  
- [Docker + docker-compose](https://docs.docker.com/) — развёртывание  
- [Open-Meteo API](https://open-meteo.com/) — источник погодных данных  
- [Jupyter Notebook](https://jupyter.org/) — визуализация  

---

## 🚀 Запуск проекта

### 1. Клонирование
```bash
git clone https://github.com/your-username/weather-data-pipeline.git
cd weather-data-pipeline
```

### 2. Настройка окружения
Создаем `.env` файл:
```env
OPENMETEO_LAT=55.75
OPENMETEO_LON=37.61
DB_USER=airflow
DB_PASSWORD=airflow
DB_HOST=postgres
DB_PORT=5432
DB_NAME=weather
```

### 3. Запуск через Docker
```bash
docker-compose up -d
```

### 4. Запуск пайплайна
Переходим в Airflow UI и запускаем DAG:
- `download_weather_dag` - скачивание данных
- `load_database_dag` - загрузка в базу
- `agg_weather_dag` - агрегация и итоговая статистика

### 5. Анализ данных
Откроем `weather_analysis.ipynb` и построим графики\прогнозы

## 📊 Результаты
- Таблица weather_raw — сырые данные
- Таблицы weather_meta_<год> — годовые агрегации
- Таблица weather_overall — сводная статистика
- Экспорт в CSV: output/weather_overall
- Графики и визуализации в Jupyter
