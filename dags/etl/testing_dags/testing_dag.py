"""
Run Source ETL with just ECS Fargate. Default way to run when the data is not big.
This is used with data providers like Bls or RH.
"""

import os
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator

from etl.common_packages.functions import ecs_parser
from etl.common_packages.cls_params import AWSDagParams

# Take the name of the file as the DagId
fileName = os.path.basename(__file__).replace(".py", "")
DAG_ID = f"cwr-{fileName}"

with DAG(
        dag_id=DAG_ID,
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        render_template_as_native_obj=True,
        catchup=False,
        default_view='tree'
) as dag:
    # Getting env from Airflow Environment Variables: 'development' | 'staging'
    # TODO: remove getting the environment from here as the best practices recommend.
    ENVIRONMENT = Variable.get('ENVIRONMENT')
    params = AWSDagParams(ENVIRONMENT)

    sync_bucket_content = BashOperator(
        task_id="sync_bucket_content",
        # bash_command="sync.sh "
        bash_command="""
                echo "Syncing Source buckets for WCM-1" && \
                aws s3 sync s3://wcm-1-source-staging/1/cwr/ s3://wcm-1-source-development/1/cwr/test/ --exclude '*' --include 'CW21003[0,1]WCM_ViMedia.V21' --copy-props none
            """
    )
