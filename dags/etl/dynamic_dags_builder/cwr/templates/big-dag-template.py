"""
<<<< Dynamically created DAG >>>>
Run Source ETL in two steps to process large amount of data.
This is used with data providers like WCM or Un
"""
import os
import pendulum

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from etl.common_packages.cls_params import AWSDagParams

from etl.common_packages.functions import ecs_parser_two_steps, emr_parser, check_running, slack_info, slack_failed

# Take the name of the file as the DagId
fileName = os.path.basename(__file__).replace(".py", "")
DAG_ID = f"cwr-{fileName}"

default_args = {
    "owner": 'airflow-$data_provider',
}

# TODO: Add composite-source step: boostrap, add step, check step
with DAG(
        dag_id=DAG_ID,
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        default_args=default_args,
        render_template_as_native_obj=True,
        catchup=False,
        is_paused_upon_creation=False
) as dag:
    # Getting env from Airflow Environment Variables: 'development' | 'staging'
    ENVIRONMENT = '$environment'
    params = AWSDagParams(ENVIRONMENT)

    start = DummyOperator(
        task_id="start",
        on_success_callback=slack_info,
    )

    get_ecs_configs = PythonOperator(
        task_id='get_ecs_configs',
        python_callable=ecs_parser_two_steps,
        op_kwargs={'input_params': params},
        depends_on_past=False,
        on_failure_callback=slack_failed,
        dag=dag)

    step1_run_fargate = ECSOperator(
        task_id="step1_run_fargate",
        dag=dag,
        on_failure_callback=slack_failed,
        depends_on_past=False,
        cluster=params.src_ecs_cluster_name,
        task_definition=params.src_ecs_task_definition,
        launch_type="FARGATE",
        overrides="{{ task_instance.xcom_pull(task_ids='get_ecs_configs', key='return_value') }}",
        awslogs_group='/ecs/cwr-job-task',
        awslogs_stream_prefix='/ecs/cwr-etl',
        network_configuration={
            'awsvpcConfiguration': {
                'subnets': params.ecs_network_subnets,
                'securityGroups': params.ecs_security_group,
                'assignPublicIp': 'ENABLED'
            }
        },
    )

    # Branch: The next task depends on the return from the python function check_running
    check_running = BranchPythonOperator(
        task_id='check_running',
        python_callable=check_running,
        on_failure_callback=slack_failed
    )

    steps_running_STOP = DummyOperator(
        task_id='steps_running_STOP'
    )

    nothing_running_continue = DummyOperator(
        task_id='nothing_running_continue'
    )

    get_emr_configs = PythonOperator(
        task_id='get_emr_configs',
        python_callable=emr_parser,
        depends_on_past=False,
        on_failure_callback=slack_failed,
        dag=dag)

    step2_start_EMR = EmrCreateJobFlowOperator(
        task_id='step2_start_EMR',
        on_failure_callback=slack_failed,
        job_flow_overrides="{{ task_instance.xcom_pull(task_ids='get_emr_configs', key='return_value') }}"
    )

    add_steps = EmrAddStepsOperator(
        task_id='add_steps',
        on_failure_callback=slack_failed,
        job_flow_id="{{ task_instance.xcom_pull(task_ids='step2_start_EMR', key='return_value') }}",
        steps="{{ task_instance.xcom_pull(task_ids='get_emr_configs', key='spark_steps') }}",
        aws_conn_id='aws_default'
    )

    copy_lookup_tables = EmrStepSensor(
        task_id='copy_lookup_tables',
        on_failure_callback=slack_failed,
        job_flow_id="{{ task_instance.xcom_pull('step2_start_EMR', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    run_src_etl = EmrStepSensor(
        task_id='run_src_etl',
        on_failure_callback=slack_failed,
        job_flow_id="{{ task_instance.xcom_pull('step2_start_EMR', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[1] }}",
        aws_conn_id='aws_default',
    )

    run_composite_src_etl = EmrStepSensor(
        task_id='run_composite_src_etl',
        on_failure_callback=slack_failed,
        job_flow_id="{{ task_instance.xcom_pull('step2_start_EMR', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[2] }}",
        aws_conn_id='aws_default',
    )

    finish = DummyOperator(
        task_id="finish",
        on_success_callback=slack_info,
    )

    start >> get_ecs_configs >> step1_run_fargate >> check_running >> steps_running_STOP
    (start >> get_ecs_configs >> step1_run_fargate >> check_running >> nothing_running_continue
     >> get_emr_configs
     >> step2_start_EMR
     >> add_steps
     >> copy_lookup_tables
     >> run_src_etl
     >> run_composite_src_etl
     >> finish)
