import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
import boto3

DATA_PROVIDER_NAME = 'rh'
DATA_PROVIDER_ID = 5
ENV = 'development'
STANDARD = 'cwr'

DATA_PROVIDER = f"{DATA_PROVIDER_NAME}-{DATA_PROVIDER_ID}"

CLUSTER_NAME = "cwr-etl-cluster"
CONTAINER_NAME = "cwr_etl"
LAUNCH_TYPE = "FARGATE"

task_definition = 'cwr-job-task:1'
network_subnets = ["subnet-0", "subnet-1"]
security_groups = ["sg-0"]

with DAG(
        dag_id="ecs_fargate_dag",
        schedule_interval=None,
        catchup=False,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
) as dag:
    client = boto3.client('ecs')
    # services = client.list_services(cluster=CLUSTER_NAME, launchType=LAUNCH_TYPE)
    # service = client.describe_services(cluster=CLUSTER_NAME, services=services['serviceArns'])

    ecs_operator_task = ECSOperator(
        task_id="ecs_operator_task",
        dag=dag,
        cluster=CLUSTER_NAME,
        # task_definition=service['services'][0]['taskDefinition'],
        task_definition=task_definition,
        launch_type=LAUNCH_TYPE,
        overrides={
            "containerOverrides": [
                {
                    "name": CONTAINER_NAME,
                    "command": ["--verbose", "false",
                                "--input_file",
                                f"s3://{DATA_PROVIDER}-source-{ENV}/{DATA_PROVIDER_ID}/cwr/CW220050RHM_000_FUND1_NEW.V21",
                                "--output_path", f"s3://{DATA_PROVIDER}-data-lake-{ENV}/",
                                "--export_path", f"s3://{DATA_PROVIDER}-exports-{ENV}/",
                                "--file_type", f"{STANDARD}",
                                "--lookup_local", "true",
                                "--data_provider_id", f"{DATA_PROVIDER_ID}"
                                ]
                },
            ],
        },

        # network_configuration=service['services'][0]['networkConfiguration'],
        network_configuration={
                'awsvpcConfiguration': {
                    'subnets': network_subnets,
                    'securityGroups': security_groups,
                    'assignPublicIp': 'ENABLED'
                }
            },
        awslogs_group=f"mwaa-ecs-{ENV}",
        awslogs_stream_prefix=f"ecs/{CONTAINER_NAME}",
    )
