import os

import boto3
import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator

from etl.common_packages.cls_params import AWSDagParams
from etl.common_packages.functions import ecs_parser

"""
Links:
    https://docs.astronomer.io/learn/dynamically-generating-dags
    https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html
    
Advantage:
    - Can parametrize the dag without limitations. Eg. can freely use the AWS parameter store.
Disadvantage:
    - Can't see the DAG code. It is harder to debug it.
    - Is this method faster or less resource consuming ? 
      Scheduler will read and generate the dag in a regular basis.
More info: https://docs.astronomer.io/learn/dynamically-generating-dags#single-file-methods    
"""


class AwsEnv:
    """
    Class to work with AWS environments
    """

    def __init__(self):
        self.client = boto3.client('ssm', region_name='us-east-1')

    @property
    def execution_environment(self):
        """
        Getting the Environment from the Parameter Store
        aws ssm get-parameters --names /common-services/aws_account_name --query "Parameters[*].{Value:Value}" --output text | cut -d, -f1| tr -d [
        """
        response = self.client.get_parameter(
            Name="/common-services/aws_account_name",
            WithDecryption=True
        )
        return response['Parameter']['Value']


def create_dag(dag_id: str,
               default_args: dict,
               ENVIRONMENT: str) -> DAG:
    with DAG(dag_id=dag_id,
             # default_view='tree',
             schedule_interval=None,
             render_template_as_native_obj=True,
             default_args=default_args) as dag:
        params = AWSDagParams(ENVIRONMENT)

        get_ecs_configs = PythonOperator(
            task_id='get_ecs_configs',
            python_callable=ecs_parser,
            op_kwargs={'input_params': params},
            depends_on_past=False,
            dag=dag)

        source_etl_fargate = ECSOperator(
            task_id="source_etl_fargate",
            dag=dag,
            depends_on_past=False,
            cluster=params.src_ecs_cluster_name,
            task_definition=params.src_ecs_task_definition,
            launch_type="FARGATE",
            overrides="{{ task_instance.xcom_pull(task_ids='get_ecs_configs', key='return_value') }}",
            # awslogs_group='/ecs/cwr-job-task',
            # awslogs_stream_prefix='ecs/cwr-etl',
            network_configuration={
                'awsvpcConfiguration': {
                    'subnets': params.ecs_network_subnets,
                    'securityGroups': params.ecs_security_group,
                    'assignPublicIp': 'ENABLED'
                }
            },
        )

        get_ecs_configs >> source_etl_fargate

    return dag


# Generating the variables needed to build the dags
# Take the name of the file as the DagId
fileName = os.path.basename(__file__).replace(".py", "")
DAG_ID = f"test-cwr-{fileName}"
aws_env = AwsEnv()
environment = aws_env.execution_environment

default_args = dict(
    owner='AirflowOrchestrator',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
)

globals()[DAG_ID] = create_dag(DAG_ID,
                               default_args,
                               environment)
