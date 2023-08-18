from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {'owner': 'airflow',
                'start_date': datetime(2021, 1, 1)
                }

with DAG(dag_id,
            schedule_interval=scheduletoreplace,
            default_args=default_args,
            catchup=False) as dag:

    t1 = PostgresOperator(
        task_id='postgres_query',
        postgres_conn_id='connection_id',
        sql=querytoreplace)
