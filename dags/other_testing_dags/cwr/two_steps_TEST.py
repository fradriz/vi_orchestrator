"""
Run Source ETL in two steps to process large amount of data.
This is used with data providers like WCM or Un

Best practices: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
"""
import os

import pendulum
from typing import List, Optional, Tuple

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable
from airflow.utils.state import State
from airflow.settings import Session
from airflow.utils.db import provide_session
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator

# from sqlalchemy import or_
try:
    from etl.configs.clusters import ETL_FARGATE_CONFIGS, SPARK_STEPS, SOURCE_ETL_EMR_CONFIGS, \
    CLUSTER_SIZE_CONFIGS
except ModuleNotFoundError:
    from dags.etl.configs.clusters import ETL_FARGATE_CONFIGS, SPARK_STEPS, SOURCE_ETL_EMR_CONFIGS, \
    CLUSTER_SIZE_CONFIGS

# Take the name of the file as the DagId
fileName = os.path.basename(__file__).replace(".py", "")
DAG_ID = f"cwr-{fileName}"

##################################### SETTING PARAMETERS FOR THE DAG #####################################
# Getting env from Airflow Environment Variables: 'development' | 'staging'
ENVIRONMENT = Variable.get('ENVIRONMENT')
# Cluster configs
fargate_configs = ETL_FARGATE_CONFIGS[ENVIRONMENT]
# Network configs
network_subnets = fargate_configs['SubnetId']
security_groups = fargate_configs['SecurityGroups']
# Source configs
source_fargate_configs = fargate_configs['source']
SOURCE_TASK_DEFINITION = source_fargate_configs['taskDefinition']
SOURCE_CLUSTER_NAME = source_fargate_configs['cluster']
SOURCE_CONTAINER_NAME = source_fargate_configs['containerOverrides']['name']
# Source configs EMR
source_emr_configs = SOURCE_ETL_EMR_CONFIGS[ENVIRONMENT]
bootstrapFile = source_emr_configs['bootstrapFile']
logUri = source_emr_configs['logUri']
# Ec2SubnetId = network_subnets       # Same as Fargate networks ...
# EmrManagedSlaveSecurityGroup = source_emr_configs['security']['EmrManagedSlaveSecurityGroup']
# EmrManagedMasterSecurityGroup = source_emr_configs['security']['EmrManagedMasterSecurityGroup']
# Composite configs
composite_src_fargate_configs = fargate_configs['composite-source']
COMPOSITE_SRC_TASK_DEFINITION = composite_src_fargate_configs['taskDefinition']
COMPOSITE_SRC_CLUSTER_NAME = composite_src_fargate_configs['cluster']
COMPOSITE_SRC_CONTAINER_NAME = composite_src_fargate_configs['containerOverrides']['name']
# Jinja parameters passed by 'trigger dags' command from Lambda trigger
# OBS: dag_run.conf can ONLY be used in templated parameters within the operators !!
BUCKET = "{{ dag_run.conf['BUCKET'] }}"
OBJECT_KEY = "{{ dag_run.conf['OBJECT_KEY'] }}"
DATA_PROVIDER = "{{ dag_run.conf['DATA_PROVIDER'] }}"
DATA_PROVIDER_ID = "{{ dag_run.conf['DATA_PROVIDER_ID'] }}"
CLUSTER_SIZE = "{{ dag_run.conf.get('CLUSTER_SIZE') }}"
## Careful: still need to be used in a templated field.
DATA_LAKE_PATH = f"s3://{DATA_PROVIDER}-{DATA_PROVIDER_ID}-data-lake-{ENVIRONMENT}/"
cluster_size = CLUSTER_SIZE_CONFIGS.get(CLUSTER_SIZE, 'small')
cluster_config = CLUSTER_SIZE_CONFIGS[cluster_size]

# agregar templated fiels a un operador que deriva del anterior ??
# class MyGoogleCloudStorageToBigQueryOperator(GoogleCloudStorageToBigQueryOperator):
#     template_fields = GoogleCloudStorageToBigQueryOperator.template_fields + ('field_delimiter',)

# EMR parameters
# JOB_FLOW_OVERRIDES = get_flow_overrides(ENVIRONMENT)
SOURCE_JOB_OVERRIDES = {
    'Name': f'{DATA_PROVIDER}-{DATA_PROVIDER_ID}-DAG_ID:{DAG_ID}-{cluster_size}',
    'LogUri': logUri,
    'ReleaseLabel': 'emr-6.4.0',
    'BootstrapActions': [{
        'Name': 'BootStrap - CWR ETL Libs',
        'ScriptBootstrapAction': {
                'Args': [ENVIRONMENT],
                'Path': bootstrapFile
        }
    }],
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': cluster_config['MasterInstanceType'],
                'InstanceCount':1,
            },
            {
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': cluster_config['InstanceType'],
                'InstanceCount': cluster_config['InstanceCount'],
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        # 'Ec2KeyName': 'mykeypair',
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole'
}
##########################################################################################################


def ti_to_tuple(ti: TaskInstance) -> Tuple:
    """
    Converts a TaskInstance in List[str] by extracting relevant bits of info from it
    :param ti: TaskInstance
    :return: List[str]
    """
    return ti.dag_id, ti.task_id, ti.state, ti.start_date


@provide_session
def get_running_step_task_instances(session: Optional[Session] = None) -> List[TaskInstance]:
    """
    https://stackoverflow.com/questions/63428936/how-to-get-the-list-of-all-the-failed-tasks-from-different-dags
    Returns list of running TaskInstance(s) for step 1
    :param session: Optional[Session]
    :return: List[TaskInstance]
    """
    # running_step_filter = or_(TaskInstance.task_id == 'step1_fargate', TaskInstance.task_id == 'step2_EMR')
    step_running_tasks = (
        session.query(TaskInstance).filter(
            TaskInstance.state == State.RUNNING,
            TaskInstance.dag_id == 'cwr_two_steps',
            TaskInstance.task_id == 'step1_fargate',
        ).all())
    return step_running_tasks


def check_running():
    """
    :return:  'still_running' or  'nothing_running'
    """
    running_step_task_instances: List[TaskInstance] = get_running_step_task_instances()
    # extract relevant bits of info from TaskInstance(s) list (to serialize them)
    step_task_instances = list(map(ti_to_tuple, running_step_task_instances))
    for running_instances in step_task_instances:
        print(f"running_instances -> {running_instances}")

    return 'still_running' if step_task_instances else 'nothing_running'


def input_parser_function(**kwargs):
    try:
        from etl.common_packages.functions import get_emr_overrides_from_base
    except ModuleNotFoundError:
        from dags.etl.common_packages.functions import get_emr_overrides_from_base

    try:
        from etl.common_packages.aws_ops import AwsOps
    except ModuleNotFoundError:
        from dags.etl.common_packages.aws_ops import AwsOps

    try:
        from etl.common_packages.functions import print_msg
    except ModuleNotFoundError:
        from dags.etl.common_packages.functions import print_msg

    try:
        from etl.configs.clusters import ETL_FARGATE_CONFIGS, SPARK_STEPS, SOURCE_ETL_EMR_CONFIGS, \
            CLUSTER_SIZE_CONFIGS, JOB_FLOW_OVERRIDES_BASE
    except ModuleNotFoundError:
        from dags.etl.configs.clusters import ETL_FARGATE_CONFIGS, SPARK_STEPS, SOURCE_ETL_EMR_CONFIGS, \
            CLUSTER_SIZE_CONFIGS, JOB_FLOW_OVERRIDES_BASE

    # Parsing from the input 'conf'
    BUCKET = kwargs['dag_run'].conf.get('BUCKET')
    OBJECT_KEY = kwargs['dag_run'].conf.get('OBJECT_KEY')
    parser = AwsOps(BUCKET, OBJECT_KEY)
    env = parser.execution_environment

    DATA_PROVIDER = parser.dataProvider
    DATA_PROVIDER_ID = parser.dataProviderId

    cluster_size = 'small'

    name = f'{DATA_PROVIDER}-{DATA_PROVIDER_ID}-DAG_ID:{DAG_ID}-{cluster_size}-{env}'
    LogUri = f's3://vm-data-provisioning-{env}/emr/logs/'

    print_msg(f"=== {name} ===")
    print_msg(f"=== {LogUri} ===")

    values_dict = {
        'Name': name,
        'LogUri': LogUri,
        'branch': env,
        'bootstrap_path': 's3://..algo/',
        'InstanceType': 'm5.4xlarge',
        'InstanceCount': 3
    }

    SOURCE_JOB_OVERRIDES = get_emr_overrides_from_base(JOB_FLOW_OVERRIDES_BASE, values_dict)
    # Need to cast 'InstanceCount' to int
    SOURCE_JOB_OVERRIDES['Instances']['InstanceGroups'][1]['InstanceCount'] = int(
        SOURCE_JOB_OVERRIDES['Instances']['InstanceGroups'][1]['InstanceCount'])

    return SOURCE_JOB_OVERRIDES


with DAG(
        dag_id=DAG_ID,
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
) as dag:
    # Step 1: Multiple ECS Fargate running in parallel
    step1_fargate = ECSOperator(
        task_id="step1_fargate",
        dag=dag,
        depends_on_past=False,
        cluster=SOURCE_CLUSTER_NAME,
        task_definition=SOURCE_TASK_DEFINITION,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": SOURCE_CONTAINER_NAME,
                    "command": ["--verbose", "false",
                                "--input_file",
                                f"s3://{BUCKET}/{OBJECT_KEY}",
                                "--output_path", f"{DATA_LAKE_PATH}/",
                                "--export_path",
                                f"s3://{DATA_PROVIDER}-{DATA_PROVIDER_ID}-exports-{ENVIRONMENT}/",
                                "--file_type", "cwr",
                                "--lookup_local", "true",
                                "--data_provider_id", f"{DATA_PROVIDER_ID}"
                                ]
                },
            ],
        },
        network_configuration={
            'awsvpcConfiguration': {
                'subnets': network_subnets,
                'securityGroups': security_groups,
                'assignPublicIp': 'ENABLED'
            }
        },
        awslogs_group=f"mwaa-ecs-{ENVIRONMENT}",
        awslogs_stream_prefix=f"ecs/{SOURCE_CONTAINER_NAME}",
    )

    # BranchPythonOperator
    # The next task depends on the return from the
    # python function check_api
    check_running = BranchPythonOperator(
        task_id='check_running',
        python_callable=check_running
    )

    still_running = DummyOperator(
        task_id='still_running'
    )

    nothing_running = DummyOperator(
        task_id='nothing_running'
    )

    get_emr_overrides_config = PythonOperator(
        task_id='get_emr_overrides_config',
        python_callable=input_parser_function,
        depends_on_past=False,
        dag=dag)

    step2_EMR = EmrCreateJobFlowOperator(
        task_id='step2_EMR',
        job_flow_overrides="{{ task_instance.xcom_pull(task_ids='get_emr_overrides_config', key='return_value') }}"
    )
    '''
    step2_EMR = EmrCreateJobFlowOperator(
        task_id='step2_EMR',
        job_flow_overrides=SOURCE_JOB_OVERRIDES
    )'''

    add_steps = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='step2_EMR', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
        # wait_for_completion=True,
    )

    watch_step = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('step2_EMR', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    step1_fargate >> check_running >> still_running
    step1_fargate >> check_running >> nothing_running >> get_emr_overrides_config >> step2_EMR >> add_steps >> watch_step
