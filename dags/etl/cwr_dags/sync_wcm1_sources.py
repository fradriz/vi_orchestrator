"""
Sync source buckets for WCM-1:
    - Source: s3://staging-sftp-files/wcm/
    - Destination: s3://wcm-1-source-ENV/1/cwr/
"""

import os
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

# Take the name of the file as the DagId
fileName = os.path.basename(__file__).replace(".py", "")
DAG_ID = f"cwr-{fileName}"

ENVIRONMENT = Variable.get('ENVIRONMENT')
source = 's3://staging-sftp-files/wcm/'
dest = f's3://wcm-1-source-{ENVIRONMENT}/1/cwr/'


if ENVIRONMENT.lower() == 'staging':
    paused_upon_creation = False
    schedule_interval = '5 4 * * *'                              # Every day at 4:05 UTC.
    sync_cmd = f"aws s3 sync {source} {dest} --exclude '*' --include 'CW*WCM_WCMGlobal.V21' --copy-props none"
else:
    paused_upon_creation = True
    schedule_interval = None
    # Testing for dev
    sync_cmd = f"aws s3 sync {source} {dest} --exclude '*' --include 'CW21005[7,8]WCM_ViMedia.V21' --copy-props none"


default_args = {
    "owner": "airflow-wcm",
    "retries": 1,
    "retry_delay": timedelta(minutes=10)
}

with DAG(
        dag_id=DAG_ID,
        schedule_interval=schedule_interval,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        default_args=default_args,
        is_paused_upon_creation=paused_upon_creation
) as dag:
    get_sftp_wcm_files = BashOperator(
        task_id="get_sftp_wcm_files",
        bash_command=f"""
                echo "Syncing source buckets for WCM-1" && {sync_cmd}
            """
    )
