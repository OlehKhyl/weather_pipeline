from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


from weather_pipeline.jobs.extract_weather import main as extract_weather_main
from weather_pipeline.jobs.load_staging_weather import main as load_staging_weather_main
from weather_pipeline.jobs.data_quality.data_quality_check_task import main as data_quality_check_task

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 27),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_extract_raw',
    default_args=default_args,
    description='A DAG to extract weather data and save to MongoDB',
    catchup=False,
    schedule='@hourly',
)

extract_weather_task = PythonOperator(
    task_id='extract_and_save_weather',
    python_callable=extract_weather_main,
    dag=dag,
)

transform_and_load = PythonOperator(
    task_id='transform_and_load_staging_weather',
    python_callable=load_staging_weather_main,
    dag=dag,
)

data_quality_check = PythonOperator(
    task_id = 'data_quality_check',
    python_callable = data_quality_check_task,
    dag=dag,
)
extract_weather_task >> transform_and_load >> data_quality_check