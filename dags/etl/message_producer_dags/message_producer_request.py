import os
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from etl.common_packages.functions import ecs_parser_mp, slack_failed, slack_info
from etl.common_packages.cls_params import AWSDagParams

# Take the name of the file as the DagId
dag_id = os.path.basename(__file__).replace(".py", "")

with DAG(dag_id=dag_id,
         schedule_interval=None,
         start_date=pendulum.datetime(2023, 3, 6, tz="UTC"),
         render_template_as_native_obj=True,
         catchup=False) as dag:
    # Getting env from Airflow Environment Variables: 'development' | 'staging'
    # TODO: remove getting the environment from here as the best practices recommend.
    ENVIRONMENT = Variable.get("ENVIRONMENT")
    params = AWSDagParams(ENVIRONMENT)

    start = DummyOperator(task_id="start",
                          on_success_callback=slack_info)

    get_ecs_configs = PythonOperator(task_id="get_ecs_configs",
                                     python_callable=ecs_parser_mp,
                                     op_kwargs={"input_params": params},
                                     depends_on_past=False,
                                     on_failure_callback=slack_failed)

    ecs_configs_override = "{{ task_instance.xcom_pull(task_ids='get_ecs_configs', key='return_value') }}"
    run_mp_fargate = ECSOperator(task_id="run_message_producer_fargate",
                                 depends_on_past=False,
                                 on_failure_callback=slack_failed,
                                 cluster=params.message_producer_ecs_cluster_name,
                                 task_definition=params.message_producer_ecs_task_definition,
                                 launch_type="FARGATE",
                                 overrides=ecs_configs_override,
                                 network_configuration={"awsvpcConfiguration": {"subnets": params.ecs_network_subnets,
                                                                                "securityGroups":
                                                                                    params.ecs_security_group,
                                                                                "assignPublicIp": "ENABLED"}})

    finish = DummyOperator(task_id="finish",
                           on_success_callback=slack_info)

    start >> get_ecs_configs >> run_mp_fargate >> finish
