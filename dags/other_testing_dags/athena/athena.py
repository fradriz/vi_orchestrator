import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
# from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
# from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
# from dags.etl.configs.athena_queries import QUERY_CREATE_TABLE
from etl.configs.schema_table.works import WORKS_SCHEMA

try:
    from etl.configs.clusters import ETL_FARGATE_CONFIGS
except ModuleNotFoundError:
    from dags.etl.configs.clusters import ETL_FARGATE_CONFIGS

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
## Careful: still need to be used in a templated field.
DATA_LAKE_PATH = f"s3://{DATA_PROVIDER}-{DATA_PROVIDER_ID}-data-lake-{ENVIRONMENT}/TEST_AIRFLOW"
##########################################################################################################
ATHENA_OUTPUT_LOCATION = f's3://vm-data-provisioning-{ENVIRONMENT}/athena/'
ENTITY = 'works'
SCHEMA = WORKS_SCHEMA
QUERY_CREATE_TABLE = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATA_PROVIDER}.{ENTITY} 
( {SCHEMA} )
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '{DATA_LAKE_PATH}/{ENTITY}/_symlink_format_manifest'
TBLPROPERTIES (
  'creator'='data_engineering')
"""


with DAG(
        dag_id="cwr_ecs_fargate",
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
) as dag:
    '''
    source_etl_fargate = ECSOperator(
        task_id="source_etl_fargate",
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

    update_athena_sources_MOCK = BashOperator(
        task_id="update_athena_sources_MOCK",
        bash_command=f"echo \"<<< UPDATE ATHENA SOURCE (MOCK) for data provider '{DATA_PROVIDER}' >>>\"",
        dag=dag,
    )'''

    athena_create_provider_table = AWSAthenaOperator(
        task_id='athena_create_provider_table',
        query=QUERY_CREATE_TABLE,
        database=DATA_PROVIDER,
        output_location=ATHENA_OUTPUT_LOCATION,
        sleep_time=30,
        max_tries=2,
        aws_conn_id='aws_default',
    )

    '''
    composite_etl_fargate = ECSOperator(
        task_id="composite_etl_fargate",
        dag=dag,
        depends_on_past=False,
        cluster=COMPOSITE_SRC_CLUSTER_NAME,
        task_definition=COMPOSITE_SRC_TASK_DEFINITION,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": COMPOSITE_SRC_CONTAINER_NAME,
                    "command": ["--verbose", "false",
                                "--dataLakePath", f"{DATA_LAKE_PATH}/{DATA_PROVIDER_ID}/data_lake/",
                                "--output_path", f"s3://composite-vi-source-{ENVIRONMENT}/vi_media/data_lake/",
                                "--whole", "true",
                                "--data_provider", f"{DATA_PROVIDER}"
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
        awslogs_stream_prefix=f"ecs/{COMPOSITE_SRC_CONTAINER_NAME}",
    )

    update_athena_composite_MOCK = BashOperator(
        task_id="update_athena_composite_MOCK",
        bash_command=f"echo \"<<< UPDATE ATHENA COMPOSITE (MOCK) for data provider '{DATA_PROVIDER}' >>>\"",
        dag=dag,
    )'''

# source_etl_fargate >> [update_athena_sources_MOCK, composite_etl_fargate]
# composite_etl_fargate >> update_athena_composite_MOCK

athena_create_provider_table
