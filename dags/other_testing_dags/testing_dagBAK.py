"""
Used for testing purposes
"""
import ast
import os
from string import Template

import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator

from etl.common_packages.functions import emr_parser

# Take the name of the file as the DagId
DAG_ID = os.path.basename(__file__).replace(".py", "")

with DAG(
        dag_id=DAG_ID,
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
) as dag:

    get_emr_overrides_config = PythonOperator(
        task_id='get_emr_overrides_config',
        python_callable=emr_parser,
        depends_on_past=False,
        dag=dag)

    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides="{{ task_instance.xcom_pull(task_ids='get_emr_overrides_config', key='return_value') }}"
    )

    add_steps = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        steps="{{ task_instance.xcom_pull(task_ids='get_emr_overrides_config', key='spark_steps') }}",
        aws_conn_id='aws_default'
    )

    check_lookup_table_step = EmrStepSensor(
        task_id='check_lookup_table_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    check_etl_step = EmrStepSensor(
        task_id='check_etl_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[1] }}",
        aws_conn_id='aws_default',
    )

get_emr_overrides_config >> cluster_creator >> add_steps >> check_lookup_table_step >> check_etl_step
