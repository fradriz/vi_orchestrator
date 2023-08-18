from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import (
    S3ToSnowflakeOperator
)
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_copy_object import (
    S3CopyObjectOperator
)
from airflow.providers.amazon.aws.operators.s3_delete_objects import (
    S3DeleteObjectsOperator
)

from datetime import datetime

@task
def get_s3_files(current_prefix):

    s3_hook = S3Hook(aws_conn_id='s3')

    current_files = s3_hook.list_keys(
        bucket_name='my-bucket',
        prefix=current_prefix+"/",
        start_after_key=current_prefix+"/"
    )

    return [[file] for file in current_files]


with DAG(
    dag_id='mapping_elt',
    start_date=datetime(2022, 4, 2),
    catchup=False,
    template_searchpath='/usr/local/airflow/include',
    schedule_interval='@daily'
) as dag:

    copy_to_snowflake_MOCK = BashOperator(
        task_id="copy_to_snowflake_MOCK",
        bash_command="echo <<< copy_to_snowflake_MOCK >>>",
        depends_on_past=True,
        dag=dag,
    )

    move_s3_MOCK = BashOperator(
        task_id="move_s3_MOCK",
        bash_command="echo \"<<< move_s3_MOCK -  {{ ds_nodash }} >>>\"",
        depends_on_past=True,
        dag=dag,
    )

    delete_landing_files_MOCK = BashOperator(
        task_id="delete_landing_files_MOCK",
        bash_command="echo \"<<< delete_landing_files_MOCK -  {{ ds_nodash }} >>>\"",
        depends_on_past=True,
        dag=dag,
    )

    transform_in_snowflake_MOCK = BashOperator(
        task_id="transform_in_snowflake_MOCK",
        bash_command="echo \"<<< transform_in_snowflake_MOCK -  {{ ds_nodash }} >>>\"",
        depends_on_past=True,
        dag=dag,
    )

    copy_to_snowflake_MOCK >> [move_s3_MOCK, transform_in_snowflake_MOCK]
    move_s3_MOCK >> delete_landing_files_MOCK
