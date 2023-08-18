from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

"""
Usage: From the UI Run the dag with this arg: { "command" : "your bash command"}
E.g. { "command" : "aws sts get-caller-identity"} or { "command" : "aws s3 ls "} or { "command" : "airflow dags list"} 

Links: 
    - https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-faqs.html
    - Airflow CLI: https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-cli-command-reference.html
"""
with DAG(dag_id="any_bash_command_dag", schedule_interval=None, catchup=False, start_date=days_ago(1)) as dag:
    cli_command = BashOperator(
        task_id="bash_command",
        bash_command="{{ dag_run.conf['command'] }}"
    )
