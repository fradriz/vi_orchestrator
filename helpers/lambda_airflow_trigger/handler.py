from utils.functions import dict_to_conf, run_airflow_cmd
from utils.s3_event_parser import S3EventParser

dag_name = 'cwr_two_steps'
mwaa_env_name = 'orchestrator-development'
mwaa_cli_command = 'dags trigger'


def airflow_trigger(event, context):
    input_event = S3EventParser(event)
    bucket = input_event.inputBucketName
    object_key = input_event.objectKey
    # data_provider = input_event.dataProvider
    # data_provider_id = input_event.dataProviderId

    # Data to send to the DAG
    dconf = {
        'BUCKET': bucket,
        'OBJECT_KEY': object_key,
        # 'DATA_PROVIDER': data_provider,
        # 'DATA_PROVIDER_ID': data_provider_id,
    }
    # Dictionary to airflow 'conf' format.
    # Remember: This data should only be used in TEMPLATED fields, it can't be used anywhere else.
    conf = dict_to_conf(dconf)
    airflow_cli_cmd = f"{mwaa_cli_command} {dag_name} -c '{conf}'"
    print(f"<<< Airflow CLI cmd to run: {airflow_cli_cmd} >>>")

    response = run_airflow_cmd(airflow_cli_cmd, mwaa_env_name)
    return response
