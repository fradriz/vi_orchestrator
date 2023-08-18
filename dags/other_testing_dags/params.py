import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="example_parameterized_dag",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)

parameterized_task = BashOperator(
    task_id="parameterized_task",
    bash_command="echo clave: {{ dag_run.conf['nombre'] }}",
    dag=dag,
)
