from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#crontab.guru

def say_yeah(name):
    print(f'{name} say yeah !!!!')

default_args = {
    'owner': 'KhoaHuynh',
    'retries': 5,
    'retry_delay': timedelta(minutes = 5)
}

with DAG(
    dag_id = 'cron_ex_id',
    default_args= default_args,
    start_date= datetime(2025, 11, 1, 1),
    schedule_interval = '0 8 * 11 *',
    description= 'Cron_Expression test',
    catchup = True
) as dag:
    say_yeah = PythonOperator(
        task_id = 'say_yeah_id',
        python_callable = say_yeah,
        op_kwargs = {'name':'Khoa Huynh'}
    )

    say_yeah