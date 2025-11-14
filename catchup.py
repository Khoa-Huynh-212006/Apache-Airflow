from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def my_dream(ti):
    ti.xcom_push(
        key = 'my_profession',
        value = 'Data Engineer'
    )
    ti.xcom_push(
        key = 'my_company',
        value = 'VNG'
    )

def dear(ti):
    profession = ti.xcom_pull(
        task_ids = 'push_data_Dream',
        key = 'my_profession'
    )
    company = ti.xcom_pull(
        task_ids = 'push_data_Dream',
        key = 'my_company'
    )
    print(f'My Dream is became {profession} in {company}')


default_args = {
    'owner': 'Khoa Huynh',
    'retries': 5,
    'retry_delay': timedelta(minutes= 1)
}

with DAG(
    dag_id ='catchup_id',
    start_date= datetime(2025, 11, 1, 1),
    schedule_interval = '@daily',
    description= 'Đây là dag cho việc học catchup',
    default_args= default_args,
    catchup = True
) as dag: 
    
    task1 = BashOperator(
        task_id = 'Introduce_my_self',
        bash_command = 'echo "My name is Khoa Huynh"'
    )

    task2 = PythonOperator(
        task_id = 'push_data_Dream',
        python_callable = my_dream
    )
    
    task3 = PythonOperator(
        task_id = 'dear_id',
        python_callable = dear
    )
    
    task2 >> task1 >> task3