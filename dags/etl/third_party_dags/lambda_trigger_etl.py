from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from etl.common_packages.functions import ecs_parser_tp
from etl.common_packages.cls_params import AWSDagParams
from airflow.models import Variable
from etl.configs.enums import Entity, Scrapers
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.operators.dummy import DummyOperator
from etl.common_packages.functions import slack_failed, slack_info


def should_run(**context):
    """
    Determine which dummy_task should be run based on if the execution date minute is even or odd.

    :param dict kwargs: Context
    :return: Id of the task to run
    :rtype: str
    """
    OBJECT_KEY = context['dag_run'].conf.get('OBJECT_KEY')
    tenant = OBJECT_KEY.split('/')[1].split('=')[1]
    entity = OBJECT_KEY.split('/')[2].split('=')[1]
    print(f'object key: {OBJECT_KEY}, start_{tenant}_{entity} is the next task to run.')
    return f'start_{tenant}_{entity}'


default_args = dict(retries=1,
                    retry_delay=timedelta(minutes=5)
                    )

with DAG(
        dag_id="third_party_etl",
        schedule_interval=None,
        start_date=datetime(2023, 2, 22),
        catchup=False,
        render_template_as_native_obj=True,
        default_args=default_args
) as dag:
    ENVIRONMENT = Variable.get('ENVIRONMENT')
    params = AWSDagParams(ENVIRONMENT)
    scrapers = {
        Scrapers.DEEZER: [Entity.RECORDING],
        Scrapers.IFPI: [Entity.RECORDING],
        Scrapers.ISWC: [Entity.WORK],
        Scrapers.SONGVIEW: [Entity.WORK]
    }

    get_ecs_configs = PythonOperator(
        task_id=f'get_ecs_configs',
        python_callable=ecs_parser_tp,
        op_kwargs={'input_params': params},
        depends_on_past=False,
        dag=dag)

    cond = BranchPythonOperator(
        task_id='condition',
        python_callable=should_run,
    )

    for tenant, entities in scrapers.items():
        for entity in entities:
            start = DummyOperator(task_id=f"start_{tenant}_{entity}",
                                  on_success_callback=slack_info)

            third_party_etl_fargate = ECSOperator(
                task_id=f"run_etl_fargate_{tenant}_{entity}",
                dag=dag,
                depends_on_past=False,
                cluster=params.src_ecs_cluster_name,
                task_definition=params.src_ecs_task_definition,
                on_failure_callback=slack_failed,
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
            finish = DummyOperator(task_id=f"finish_{tenant}_{entity}",
                                   on_success_callback=slack_info)

            get_ecs_configs >> cond >> start >> third_party_etl_fargate >> finish
