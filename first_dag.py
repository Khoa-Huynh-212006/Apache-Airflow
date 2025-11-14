from airflow import DAG

from airflow.operators.bash import BashOperator 

from datetime import timedelta, datetime


default_args = {
    'owner': 'Khoa Huynh',
    'retries': 5, 
    'retry_deley': timedelta(minutes= 1)
}

with DAG(
    dag_id= '1st_dag',
    default_args= default_args,
    schedule_interval = '@daily',
    description= 'Đây là DAG đầu tiên của Khoa Huynh',
    start_date= datetime(2025, 11, 9, 1)
) as dag:
    
    task1 = BashOperator(
        task_id = '1st_task',
        bash_command = "echo 'Hello My name is Khoa, This is 1st task'"
    )

    task2 = BashOperator(
        task_id = 'second_task',
        bash_command = "echo second_task"
    )

    task3 = BashOperator(
        task_id = 'third_task',
        bash_command = "echo third_task"
    )
    

    task1 >> task2
    task1 >> task3