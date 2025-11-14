from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator


from datetime import timedelta, datetime


default_args = {
    'owner': 'KhoaHuynh',
    'retries': 5,
    'retry_delay': timedelta(minutes = 5)
}

with DAG(
    dag_id = 'postgres_id12',
    default_args= default_args,
    start_date= datetime(2025, 11, 1, 1),
    schedule_interval = '@daily',
    description= 'Postgres test',
    catchup = True
) as dag:

    task1 = PostgresOperator(
        task_id = 'postgres_task_id',
        postgres_conn_id = 'postgres_localhost',
        sql = 
        """ 
            create table if not exists dag_runs(
            dt_date date,
            dag_id character varying,
            primary key (dt_date, dag_id)
            )
        """
    )
    task1
