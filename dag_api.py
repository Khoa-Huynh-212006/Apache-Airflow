from airflow import DAG 
from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'Khoa',
    'retries': 5,
    'retry_delay': timedelta(minutes= 1)
}

@dag(
    dag_id = 'api_dag_id',
    default_args = default_args,
    start_date = datetime(2025, 11, 9, 1),
    schedule_interval = '@daily'
)
def hello_api():

    @task()
    def get_name():
        return 'KhoaHuynh'
    
    @task()
    def get_age():
        return 19
    
    @task()
    def say_hi(name, age):
        print(f"Tôi là {name}, tôi {age} tuổi")

    name = get_name()
    age = get_age()
    say_hi(name, age)

my_pipeline = hello_api