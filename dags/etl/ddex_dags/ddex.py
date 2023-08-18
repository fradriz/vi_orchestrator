"""
DDEX Standard
Run Source ETL with ECS Fargate.
Slack Formats: https://slack.com/help/articles/202288908-Format-your-messages
"""
import os
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator

from etl.common_packages.functions import ecs_parser_xlsx, slack_failed, slack_info, ecs_parser_ddex
from etl.common_packages.cls_params import AWSDagParams

# Take the name of the file as the DagId
fileName = os.path.basename(__file__).replace(".py", "")
DAG_ID = f"{fileName}"


with DAG(
        dag_id=DAG_ID,
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        render_template_as_native_obj=True,
        catchup=False,
) as dag:
    # Getting env from Airflow Environment Variables: 'development' | 'staging'
    ENVIRONMENT = Variable.get('ENVIRONMENT')
    params = AWSDagParams(ENVIRONMENT)

    start = DummyOperator(
        task_id="start",
        on_success_callback=slack_info,
    )

    get_ecs_configs = PythonOperator(
        task_id='get_ecs_configs',
        python_callable=ecs_parser_ddex,
        op_kwargs={'input_params': params},
        depends_on_past=False,
        # on_success_callback=slack_info,
        on_failure_callback=slack_failed,
        dag=dag)

    source_etl_fargate = ECSOperator(
        task_id="source_etl_fargate",
        dag=dag,
        depends_on_past=False,
        # on_success_callback=slack_info,
        on_failure_callback=slack_failed,
        cluster=params.src_ecs_cluster_name,
        task_definition=params.src_ecs_task_definition,
        launch_type="FARGATE",
        overrides="{{ task_instance.xcom_pull(task_ids='get_ecs_configs', key='return_value') }}",
        network_configuration={
            'awsvpcConfiguration': {
                'subnets': params.ecs_network_subnets,
                'securityGroups': params.ecs_security_group,
                'assignPublicIp': 'ENABLED'
            }
        },
    )

    composite_source_etl_fargate = ECSOperator(
        task_id="composite_source_etl_fargate",
        dag=dag,
        depends_on_past=False,
        on_failure_callback=slack_failed,
        cluster=params.comp_src_ecs_cluster_name,
        task_definition=params.comp_src_ecs_task_definition,
        launch_type="FARGATE",
        overrides="{{ task_instance.xcom_pull(task_ids='get_ecs_configs', key='composite_source_params') }}",
        network_configuration={
            'awsvpcConfiguration': {
                'subnets': params.ecs_network_subnets,
                'securityGroups': params.ecs_security_group,
                'assignPublicIp': 'ENABLED'
            }
        },
    )

    finish = DummyOperator(
        task_id="finish",
        on_success_callback=slack_info,
    )

    start >> get_ecs_configs >> source_etl_fargate >> composite_source_etl_fargate >> finish
