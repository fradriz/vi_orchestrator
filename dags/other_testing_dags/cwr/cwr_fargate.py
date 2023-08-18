import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from common_packages.wcm import DATA_PROVIDER
from common_packages.clusters import SOURCE_ETL_FARGATE_CONFIGS


ENV = 'development'
fargate_configs = SOURCE_ETL_FARGATE_CONFIGS[ENV]
task_definition = fargate_configs['taskDefinition']
CLUSTER_NAME = fargate_configs['cluster']


def run_composite_source_etl(**kwargs):
    print(f"<<< Running composite-source ETL for {kwargs['dag_run'].conf.get('data_provider_name')} >>>")
    print(f"\t<<< dag_run:--{kwargs['dag_run'].conf}-- >>>")
    print(f"\t<<< ARGS1:--{kwargs['name']}-- >>>")
    # print(f"\t<<< dag_run.conf:--{kwargs['dag_run'].conf.get('data_provider_name')}-- >>>")
    # raise ValueError('This will turns the python task in failed state')
    # return 0


with DAG(
    dag_id="cwr_fargate_flow",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

    poc_run_etl = BashOperator(
        task_id="poc_run_etl",
        bash_command="echo \"<<< Source ETL - DAG_RUN: {{ dag_run.conf }} >>>\"",
        depends_on_past=True,
        dag=dag,
    )

    poc_update_athena_sources = BashOperator(
        task_id="poc_update_athena_sources",
        bash_command=f"echo \"<<< Sources - Athena update for \'{DATA_PROVIDER}\'>>>\"",
        dag=dag,
    )

    poc_run_composite_etl = PythonOperator(
        task_id='poc_run_composite_etl',
        dag=dag,
        python_callable=run_composite_source_etl,
        op_kwargs={'clusterName': CLUSTER_NAME, 'TASK': f'-->{task_definition}'},
        depends_on_past=True)

    poc_update_athena_composite = BashOperator(
        task_id="poc_update_athena_composite",
        bash_command=f"echo \"<<< athena upate composite sources for \'{DATA_PROVIDER}\' >>>\"",
        dag=dag,
    )


poc_run_etl >> [poc_update_athena_sources, poc_run_composite_etl]
poc_run_composite_etl >> poc_update_athena_composite
