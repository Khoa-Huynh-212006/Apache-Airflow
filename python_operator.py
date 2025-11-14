from airflow import DAG 

from airflow.operators.python import PythonOperator 

from datetime import datetime, timedelta


def say_hi(ti):
    name = ti.xcom_pull(task_ids = "name_age_input",
                        key = 'my_name')
    age = ti.xcom_pull(task_ids = "name_age_input",
                       key = 'my_age')
    print(f"My name is {name}, I'm {age} years old")

def get_name_age(ti):
    ti.xcom_push(key = 'my_name',
                 value = 'Khoa_Huynh')
    ti.xcom_push(key = 'my_age',
                 value = 19)

default_args = {
    'owner': 'Khoa Huynh',
    'retries': 5,
    'retry_delay': timedelta(minutes= 1)
}


with DAG(
    dag_id = 'dag_python',
    default_args= default_args,
    start_date= datetime(2025, 11, 9,1),
    schedule_interval = '@daily',
    description = "python airflow có đỉnh không ?"
) as dag:
    task1 = PythonOperator(
        task_id = '1st_taskpy',
        python_callable = say_hi
    )

    task2 = PythonOperator(
        task_id = "name_age_input",
        python_callable = get_name_age
    )

    task2 >> task1