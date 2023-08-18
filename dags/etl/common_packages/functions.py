import ast
import re
import boto3
import typing as tp
from string import Template
from datetime import datetime
from airflow.models import Variable

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.state import State
from airflow.models import TaskInstance
from airflow.settings import Session
from airflow.utils.session import provide_session
from etl.configs.enums import ExecutionType, Standard
from etl.common_packages.aws_ops import AwsOps
from etl.configs.clusters import DATA_PROVIDERS_CONF, CLUSTER_SIZE_CONFIGS, \
    SPARK_STEPS_2_BASE, JOB_FLOW_OVERRIDES_BASE, SOURCE_FARGATE_OVERRIDES_BASE, \
    COMPOSITE_SOURCE_FARGATE_OVERRIDES_BASE, SOURCE_FARGATE_THIRD_PARTY_OVERRIDES_BASE, \
    MESSAGE_PRODUCER_FARGATE_OVERRIDES_BASE, DDEX_FARGATE_OVERRIDES_BASE, DDEX_PREPRO_FARGATE_OVERRIDES_BASE
from sqlalchemy import or_


ENVIRONMENT = Variable.get('ENVIRONMENT')
if ENVIRONMENT == 'development':
    SLACK_CHANNEL = 'C04QF0ZGSJW'   # development - '#ingestion-pipeline-dev'
else:
    SLACK_CHANNEL = 'C04UC7L8SE4'   # staging - '#ingestion-pipeline'


def print_msg(msg, head=None, str_size=120):
    if head:
        str_size = str_size - len(head)
        new_str_size = str_size // 2
        lines = "_" * new_str_size
        head_str = lines + head + lines
        print(head_str)
        print(msg)
        print("_" * len(head_str))
    else:
        print("_" * str_size)
        print(msg)
        print("_" * str_size)


def get_execution_environment():
    """
    Getting the Environment from the Parameter Store
    """
    client = boto3.client('ssm', region_name='us-east-1')
    response = client.get_parameter(
        Name="/common-services/aws_account_name",
        WithDecryption=True
    )
    return response['Parameter']['Value']


# ######################### Input Sequence for CWR two Steps ##########################
def fill_dict_template(template_dict: dict, values_dict: dict) -> dict:
    """
    Function to replace/fill values in a dict template.
    === Usage ===

    template_dict = {
        'Name': '$Name_of',
        'LogUri': '$LogUri'
        }
    values_dict = {'Name_of': 'any Name', 'LogUri': 's3:/../smth'}

    output_dict = {
        'Name': 'any Name',
        'LogUri': 's3:/../smth'
        }


    :param template_dict: Templated o base dictionary
    :param values_dict: Values to replace in the base dictionary
    :return: Final dictionary with new values
    """
    # Dict to Str
    template_dict_STR = str(template_dict)
    # Replacing the designated fields
    final_output_STR = Template(template_dict_STR).safe_substitute(values_dict)
    # Output: Str to Dict
    return ast.literal_eval(final_output_STR)


def get_s3_filelist(bucket: str, prefix: str) -> tp.List[int]:
    """
    Read S3 and return a sorted list of elements to process in EMR.
    Eg.
    """

    def _get_filename(file_path):
        try:
            file_seq_num = re.findall(r'/filename=CW(\d+).+/', file_path, re.IGNORECASE)[0]

        except Exception as e:
            raise Exception(f"Can't read the file sequence number from '{file_path}' - {e}")

        return int(file_seq_num)

    s3 = boto3.client('s3')
    print(f"Function: 'get_s3_filelist' - Reading: 's3://{bucket}/{prefix}'")
    contents = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents")
    s3_files_list = (_get_filename(content['Key']) for content in contents if ('filename=' in content['Key']))

    return sorted(set(s3_files_list))


def get_input_sequence(
        first_file: int = 1,
        last_file: int = 200,
        year: int = None) -> str:
    """
    ### Used for WCM data ###
    Create a list of files to test when calling EMR.
    :param first_file:
    :param last_file:
    :param year: Generate the current year unless specified. E.g. If 'year=21', it will generate 2021 files.
    return: File sequence of strings. E.g. "--input_file", "210041, 210042, 210043, 210044, 210045"
    """
    YYc = str(datetime.utcnow().strftime("%y"))             # YYc = current year
    YY_process = str(year) if year else int(YYc)

    input_file_list = sorted([int(f"{YY_process}{n:04d}") for n in range(first_file, last_file + 1)])
    input_file_str = str(input_file_list).replace('[', '').replace(']', '')

    return input_file_str


def get_input_seq_list(input_event: AwsOps) -> str:
    if 'un' in input_event.dataProvider:
        preprod_bucket = input_event.data_lake_bucket
        preprod_prefix = input_event.preProdKey
        print("Reading preProd and get the list of processed files")
        input_seq_list = get_s3_filelist(preprod_bucket, preprod_prefix)

        if input_seq_list:
            input_seq_str = str(input_seq_list).replace('[', '').replace(']', '')
            print(f"Un File List for EMR: '{input_seq_str}'")
        else:
            raise Exception(f"Can't read Un files from: {preprod_bucket}/{preprod_prefix}")
    else:
        input_seq_str = get_input_sequence()
        # WCM Special case: Process the last three files for WCMGlobal in 2021
        # input_seq_str = "CW210037WCM_WCMGlobal, CW210038WCM_WCMGlobal, CW210039WCM_WCMGlobal"

    return input_seq_str


# ######################### Input Parsers ##########################
def ecs_parser(ti, **context):
    """
    Parser used for CWR small data
    :param ti: TaskInstance
    :param context: Airflow context
    :return:
    """
    # Jinja: Parsing from the input 'conf' sent by the trigger
    BUCKET = context['dag_run'].conf.get('BUCKET')
    OBJECT_KEY = context['dag_run'].conf.get('OBJECT_KEY')
    parser = AwsOps(BUCKET, OBJECT_KEY)

    DATA_PROVIDER = parser.dataProvider
    dp_keys = DATA_PROVIDERS_CONF.keys()
    dp = DATA_PROVIDER if DATA_PROVIDER in dp_keys else 'default'
    data_provider_conf = DATA_PROVIDERS_CONF.get(dp)
    execution_type = data_provider_conf['execution_type']
    dag_id = ti.dag_id.lower()
    file_type = parser.standard

    print(f"Running {execution_type} for {DATA_PROVIDER} ({dp}) - Standard = {file_type}")

    if ('big' in dag_id or 'test' in dag_id) and (file_type == 'CWR') and (execution_type == ExecutionType.fargateEMR):
        etl_steps = '1/2'
        # elif ('medium' in dag_id or 'test' in dag_id) and execution_type == ExecutionType.fargate:
        #    etl_steps = 'std'
    else:
        etl_steps = 'std'
        # raise Exception(f"Data Provider '{DATA_PROVIDER}' cannot execute {execution_type}'. "
        #                f"Check 'etl.configs.clusters.py' file to include it.")

    params = context['input_params']
    # Fargate Source ETL Params
    overrides_values = {
        'container_name': params.src_ecs_container_name,
        'BUCKET': BUCKET,
        'OBJECT_KEY': OBJECT_KEY,
        'DATA_PROVIDER': parser.dataProvider,
        'DATA_PROVIDER_ID': parser.dataProviderId,
        'ETL_STEPS': etl_steps,
        'ENV': parser.execution_environment,
        'FILE_TYPE': file_type
    }
    SOURCE_FARGATE_OVERRIDES = fill_dict_template(
        template_dict=SOURCE_FARGATE_OVERRIDES_BASE,
        values_dict=overrides_values)

    if etl_steps == 'std':
        # For Big data executions this step is going to be added in EMR and not here.
        # Fargate Composite Source ETL Params
        overrides_values = {
            'container_name': params.comp_src_ecs_container_name,
            'BUCKET': BUCKET,
            'OBJECT_KEY': OBJECT_KEY,
            'DATA_PROVIDER': parser.dataProvider,
            'DATA_PROVIDER_ID': parser.dataProviderId,
            'ENV': parser.execution_environment
        }
        COMPOSITE_SOURCE_FARGATE_OVERRIDES = fill_dict_template(
            template_dict=COMPOSITE_SOURCE_FARGATE_OVERRIDES_BASE,
            values_dict=overrides_values)

        ti.xcom_push(key='composite_source_params', value=COMPOSITE_SOURCE_FARGATE_OVERRIDES)

    # Return value to XCOM
    return SOURCE_FARGATE_OVERRIDES


def ecs_parser_xlsx(ti, **context):
    """
    Parser used for XLSX small data

    Expected input:
        BUCKET:xlsx-ingestions (bucket in Dotbc account)
        OBJECT_KEY:xlsx-ingestions/wcm-187/vi/

    :param ti: TaskInstance
    :param context: Airflow context
    :return:
    """
    # Jinja: Parsing from the input 'conf' sent by the trigger
    BUCKET = context['dag_run'].conf.get('BUCKET')
    OBJECT_KEY = context['dag_run'].conf.get('OBJECT_KEY')
    parser = AwsOps(BUCKET, OBJECT_KEY)

    DATA_PROVIDER = parser.dataProvider
    dp_keys = DATA_PROVIDERS_CONF.keys()
    dp = DATA_PROVIDER if DATA_PROVIDER in dp_keys else 'default'
    data_provider_conf = DATA_PROVIDERS_CONF.get(dp)
    execution_type = data_provider_conf['execution_type']
    # dag_id = ti.dag_id.lower()
    file_type = parser.standard
    etl_steps = 'std'
    print(f"Running {execution_type} for {DATA_PROVIDER} ({dp}) - Standard = {file_type}")

    params = context['input_params']
    # Fargate Source ETL Params
    overrides_values = {
        'container_name': params.src_ecs_container_name,
        'BUCKET': BUCKET,
        'OBJECT_KEY': OBJECT_KEY,
        'DATA_PROVIDER': parser.dataProvider,
        'DATA_PROVIDER_ID': parser.dataProviderId,
        'ETL_STEPS': etl_steps,
        'ENV': parser.execution_environment,
        'FILE_TYPE': file_type
    }
    SOURCE_FARGATE_OVERRIDES = fill_dict_template(
        template_dict=SOURCE_FARGATE_OVERRIDES_BASE,
        values_dict=overrides_values)

    overrides_values = {
        'container_name': params.comp_src_ecs_container_name,
        'BUCKET': BUCKET,
        'OBJECT_KEY': OBJECT_KEY,
        'DATA_PROVIDER': parser.dataProvider,
        'DATA_PROVIDER_ID': parser.dataProviderId,
        'ENV': parser.execution_environment
    }
    COMPOSITE_SOURCE_FARGATE_OVERRIDES = fill_dict_template(
        template_dict=COMPOSITE_SOURCE_FARGATE_OVERRIDES_BASE,
        values_dict=overrides_values)

    ti.xcom_push(key='composite_source_params', value=COMPOSITE_SOURCE_FARGATE_OVERRIDES)

    # Return value to XCOM
    return SOURCE_FARGATE_OVERRIDES


def ecs_parser_ddex(ti, **context):
    """
    Parser used for DDEX standard.

    Expected input:
        BUCKET:<data_provider>-<data_provider_id>-source-<ENV> (ie. monstercat-55-source-development)
        OBJECT_KEY:launcherState/specificationFiles/pending/specificationFile.*.json
            (ie. launcherState/specificationFiles/pending/specificationFile.2023-03-22T17:23:46.json)

    :param ti: TaskInstance
    :param context: Airflow context
    :return:
    """
    # Jinja: Parsing from the input 'conf' sent by the trigger
    BUCKET = context['dag_run'].conf.get('BUCKET')
    OBJECT_KEY = context['dag_run'].conf.get('OBJECT_KEY')
    parser = AwsOps(BUCKET, OBJECT_KEY)

    DATA_PROVIDER = parser.dataProvider
    dp_keys = DATA_PROVIDERS_CONF.keys()
    dp = DATA_PROVIDER if DATA_PROVIDER in dp_keys else 'default'
    data_provider_conf = DATA_PROVIDERS_CONF.get(dp)
    execution_type = data_provider_conf['execution_type']
    # dag_id = ti.dag_id.lower()
    file_type = Standard.DDEX
    print(f"Running {execution_type} for {DATA_PROVIDER} ({dp}) - Standard = {file_type}")

    params = context['input_params']

    # Fargate Source ETL Params
    overrides_values = {
        'container_name': params.src_ecs_container_name,
        'BUCKET': BUCKET,
        'OBJECT_KEY': OBJECT_KEY,
        'DATA_PROVIDER': parser.dataProvider,
        'DATA_PROVIDER_ID': parser.dataProviderId,
        'ENV': parser.execution_environment,
        'FILE_TYPE': file_type
    }
    SOURCE_FARGATE_OVERRIDES = fill_dict_template(
        template_dict=DDEX_FARGATE_OVERRIDES_BASE,
        values_dict=overrides_values)

    # Composite ECS
    overrides_values = {
        'container_name': params.comp_src_ecs_container_name,
        'BUCKET': BUCKET,
        'OBJECT_KEY': OBJECT_KEY,
        'DATA_PROVIDER': parser.dataProvider,
        'DATA_PROVIDER_ID': parser.dataProviderId,
        'ENV': parser.execution_environment
    }
    COMPOSITE_SOURCE_FARGATE_OVERRIDES = fill_dict_template(
        template_dict=COMPOSITE_SOURCE_FARGATE_OVERRIDES_BASE,
        values_dict=overrides_values)

    # Sending composite values to XCOM
    ti.xcom_push(key='composite_source_params', value=COMPOSITE_SOURCE_FARGATE_OVERRIDES)
    # Returning source values to XCOM
    return SOURCE_FARGATE_OVERRIDES


def ecs_parser_ddex_prepro(ti, **context):
    """
    Parser used for DDEX preprocessing. Copy relevant files from DotBc into the environment (development | staging).
    Main environment: development (this is ought to be scheduled). Todo: move this to staging.
    Affected client: STMPD.

    Expected input:
        BUCKET:<data_provider>-<data_provider_id>-source-<ENV> (ie. monstercat-55-source-development)
        OBJECT_KEY:launcherState/specificationFiles/pending/specificationFile.*.json
            (ie. launcherState/specificationFiles/pending/specificationFile.2023-03-22T17:23:46.json)

    :param ti: TaskInstance
    :param context: Airflow context
    :return:
    """
    # Jinja: Parsing from the input 'conf' sent by the trigger
    BUCKET = context['dag_run'].conf.get('BUCKET')
    OBJECT_KEY = context['dag_run'].conf.get('OBJECT_KEY')
    parser = AwsOps(BUCKET, OBJECT_KEY)

    DATA_PROVIDER = parser.dataProvider
    dp_keys = DATA_PROVIDERS_CONF.keys()
    dp = DATA_PROVIDER if DATA_PROVIDER in dp_keys else 'default'
    data_provider_conf = DATA_PROVIDERS_CONF.get(dp)
    execution_type = data_provider_conf['execution_type']
    # dag_id = ti.dag_id.lower()
    file_type = Standard.DDEX
    print(f"Running {execution_type} for {DATA_PROVIDER} ({dp}) - Standard = {file_type}")

    params = context['input_params']

    # Fargate Source ETL Params
    overrides_values = {
        'container_name': params.src_ecs_container_name,
        'BUCKET': BUCKET,
        'OBJECT_KEY': OBJECT_KEY,
        'DATA_PROVIDER': parser.dataProvider,
        'DATA_PROVIDER_ID': parser.dataProviderId,
        'ENV': parser.execution_environment,
        'FILE_TYPE': file_type
    }
    SOURCE_FARGATE_OVERRIDES = fill_dict_template(
        template_dict=DDEX_PREPRO_FARGATE_OVERRIDES_BASE,
        values_dict=overrides_values)

    # Composite ECS
    overrides_values = {
        'container_name': params.comp_src_ecs_container_name,
        'BUCKET': BUCKET,
        'OBJECT_KEY': OBJECT_KEY,
        'DATA_PROVIDER': parser.dataProvider,
        'DATA_PROVIDER_ID': parser.dataProviderId,
        'ENV': parser.execution_environment
    }
    COMPOSITE_SOURCE_FARGATE_OVERRIDES = fill_dict_template(
        template_dict=COMPOSITE_SOURCE_FARGATE_OVERRIDES_BASE,
        values_dict=overrides_values)

    # Sending composite values to XCOM
    ti.xcom_push(key='composite_source_params', value=COMPOSITE_SOURCE_FARGATE_OVERRIDES)
    # Returning source values to XCOM
    return SOURCE_FARGATE_OVERRIDES

def ecs_parser_tp(ti, **context):
    """
    Parser used for CWR small data
    :param ti: TaskInstance
    :param context: Airflow context
    :return:
    """
    # Jinja: Parsing from the input 'conf' sent by the trigger
    BUCKET = context['dag_run'].conf.get('BUCKET')
    OBJECT_KEY = context['dag_run'].conf.get('OBJECT_KEY')
    parser = AwsOps(BUCKET, OBJECT_KEY)

    DATA_PROVIDER = 'third-party'
    dp_keys = DATA_PROVIDERS_CONF.keys()
    dp = DATA_PROVIDER if DATA_PROVIDER in dp_keys else 'default'
    data_provider_conf = DATA_PROVIDERS_CONF.get(dp)
    execution_type = data_provider_conf['execution_type']

    print(f"Running {execution_type} for {DATA_PROVIDER} ({dp})")

    params = context['input_params']
    # Fargate Source ETL Params
    overrides_values = {
        'container_name': params.src_ecs_container_name,
        'BUCKET': BUCKET,
        'OBJECT_KEY': OBJECT_KEY,
        'DATA_PROVIDER': DATA_PROVIDER,
        'ENV': parser.execution_environment,
        'TENANT': OBJECT_KEY.split('/')[1].split('=')[1]
    }
    SOURCE_FARGATE_OVERRIDES = fill_dict_template(
        template_dict=SOURCE_FARGATE_THIRD_PARTY_OVERRIDES_BASE,
        values_dict=overrides_values)

    # Return value to XCOM
    return SOURCE_FARGATE_OVERRIDES


def ecs_parser_mp(**context) -> tp.Dict[str, tp.Any]:
    """
    Parser used for Message Producer request
    :param context: Airflow context
    :return: Parsed ECS Container Overrides Dictionary.
    :rtype: dict
    """
    # Jinja: Parsing from the input 'conf' sent by the trigger
    bucket_name = context["dag_run"].conf.get("bucket")
    object_key = context["dag_run"].conf.get("object_key")
    if bucket_name is not None and object_key is not None:
        parser = AwsOps(bucket_name, object_key)
        data_provider = parser.dataProvider
        data_provider_id = parser.dataProviderId
        execution_environment = parser.execution_environment
    else:
        data_provider = context["dag_run"].conf.get("data_provider")
        data_provider_id = context["dag_run"].conf.get("data_provider_id")
        execution_environment = context["dag_run"].conf.get("environment")

    file_standard = context["dag_run"].conf.get("file_standard")
    message_type = context["dag_run"].conf.get("message_type")
    timestamp_run = context["dag_run"].conf.get("timestamp_run")
    dry_run = context["dag_run"].conf.get("dry_run")
    dry_run = dry_run if dry_run is not None else False

    params = context['input_params']
    # Fargate Message Producer Params
    override_values = {"CONTAINER_NAME": params.message_producer_ecs_container_name,
                       "DATA_PROVIDER": data_provider,
                       "DATA_PROVIDER_ID": data_provider_id,
                       "ENV": execution_environment,
                       "FILE_STANDARD": file_standard,
                       "MESSAGE_TYPE": message_type,
                       "TIMESTAMP_RUN_FLAG": "--timestampRun" if timestamp_run is not None else "",
                       "TIMESTAMP_RUN_VALUE": timestamp_run if timestamp_run is not None else "",
                       "DRY_RUN": str(dry_run).lower()}
    MESSAGE_PRODUCER_FARGATE_OVERRIDES = fill_dict_template(template_dict=MESSAGE_PRODUCER_FARGATE_OVERRIDES_BASE,
                                                            values_dict=override_values)
    # Remove empty string arguments
    command = MESSAGE_PRODUCER_FARGATE_OVERRIDES["containerOverrides"][0]["command"]
    command = [v for v in command if v]
    MESSAGE_PRODUCER_FARGATE_OVERRIDES["containerOverrides"][0]["command"] = command

    # Return value to XCOM
    return MESSAGE_PRODUCER_FARGATE_OVERRIDES


def ecs_parser_two_steps(ti, **context):
    """
        Parser used for CWR medium/big data (i.e. whole catalog)
        :param ti:
        :param context:
        :return
    """
    print_msg("ETL CloudWatch: 'aws logs tail /ecs/cwr-job-task --follow --format short --since 1m'")
    # Jinja: Parsing from the input 'conf' sent by the trigger
    BUCKET = context['dag_run'].conf.get('BUCKET')
    OBJECT_KEY = context['dag_run'].conf.get('OBJECT_KEY')
    parser = AwsOps(BUCKET, OBJECT_KEY)
    DATA_PROVIDER = parser.dataProvider

    dp_keys = DATA_PROVIDERS_CONF.keys()
    dp = DATA_PROVIDER if DATA_PROVIDER in dp_keys else 'default'
    data_provider_conf = DATA_PROVIDERS_CONF.get(dp)

    dag_id = ti.dag_id.lower()
    if ('two_steps' in dag_id or 'emr' in dag_id) and ('EMR' not in data_provider_conf['execution_type']):
        raise Exception(f"Data Provider '{DATA_PROVIDER}' cannot execute ETL in 'EMR' or 'two steps'. "
                        f"Check 'etl.configs.clusters.py' file to include it.")

    params = context['input_params']
    overrides_values = {
        'container_name': params.src_ecs_container_name,
        'BUCKET': BUCKET,
        'OBJECT_KEY': OBJECT_KEY,
        'DATA_PROVIDER': parser.dataProvider,
        'DATA_PROVIDER_ID': parser.dataProviderId,
        'ETL_STEPS': '1/2',
        'ENV': parser.execution_environment,
        'FILE_TYPE': parser.standard
    }
    SOURCE_STEP1_FARGATE_OVERRIDES = fill_dict_template(
        template_dict=SOURCE_FARGATE_OVERRIDES_BASE,
        values_dict=overrides_values)

    # Return value is going to XCOM too
    return SOURCE_STEP1_FARGATE_OVERRIDES


def emr_parser(ti, **context):
    # Jinja: Parsing from the input 'conf'
    BUCKET = context['dag_run'].conf.get('BUCKET')
    OBJECT_KEY = context['dag_run'].conf.get('OBJECT_KEY')
    parser = AwsOps(BUCKET, OBJECT_KEY)
    env = parser.execution_environment
    standard = parser.standard

    DATA_PROVIDER = parser.dataProvider
    DATA_PROVIDER_ID = parser.dataProviderId
    FULL_DATA_PROVIDER = f"{DATA_PROVIDER}-{DATA_PROVIDER_ID}"

    dp_keys = DATA_PROVIDERS_CONF.keys()
    print_msg(dp_keys)
    dp = DATA_PROVIDER if DATA_PROVIDER in dp_keys else 'default'
    print_msg(dp)
    data_provider_conf = DATA_PROVIDERS_CONF.get(dp)
    print_msg(data_provider_conf)
    cluster_size = data_provider_conf['cluster_size']
    print_msg(cluster_size)
    cluster_configs = CLUSTER_SIZE_CONFIGS[cluster_size]
    print_msg(dp)

    if 'EMR' not in data_provider_conf['execution_type']:
        raise Exception(f"Data Provider '{DATA_PROVIDER}' is not allowed to run in EMR. "
                        f"Check 'etl.configs.clusters.py' file to change this behavior.")

    # Overrides
    ovr_values_dict = {
        'Name': f'Airflow-{FULL_DATA_PROVIDER}-{cluster_size}-{env}',
        'env': env,
        'InstanceType': cluster_configs['InstanceType'],
        'InstanceCount': cluster_configs['InstanceCount']
    }

    SOURCE_JOB_OVERRIDES = fill_dict_template(
        template_dict=JOB_FLOW_OVERRIDES_BASE,
        values_dict=ovr_values_dict)
    # Cast 'InstanceCount' to int
    SOURCE_JOB_OVERRIDES['Instances']['InstanceGroups'][1]['InstanceCount'] = int(
        SOURCE_JOB_OVERRIDES['Instances']['InstanceGroups'][1]['InstanceCount'])

    # Spark Steps
    input_seq = get_input_seq_list(parser)
    steps_values_dict = {
        'file_type': standard,
        'input_files': input_seq,
        'DATA_PROVIDER': DATA_PROVIDER,
        'DATA_PROVIDER_ID': DATA_PROVIDER_ID,
        'ENV': env
    }
    SPARK_STEPS = fill_dict_template(
        template_dict=SPARK_STEPS_2_BASE,
        values_dict=steps_values_dict)

    # Pushing STEPS to XCOM Metadata DB
    # context['ti'].xcom_push(key='spark_steps', value=SPARK_STEPS)
    ti.xcom_push(key='spark_steps', value=SPARK_STEPS)

    # Return value is going to XCOM too
    return SOURCE_JOB_OVERRIDES


# ######################### CHECK RUNNING FUNCTIONS ##########################
def ti_to_tuple(ti: TaskInstance) -> tp.Tuple:
    """
    Converts a TaskInstance in List[str] by extracting relevant bits of info from it
    :param ti: TaskInstance
    :return: List[str]
    """
    return ti.dag_id, ti.task_id, ti.state, ti.start_date


@provide_session
def get_running_step_task_instances(dag_id: str, session: tp.Optional[Session] = None) -> tp.List[TaskInstance]:
    """
    https://stackoverflow.com/questions/63428936/how-to-get-the-list-of-all-the-failed-tasks-from-different-dags
    Returns list of running TaskInstance(s) for step 1
    :param dag_id: current dag id
    :param session: Optional[Session]
    :return: List[TaskInstance]
    """
    # If any of these steps are running then do not launch the EMR job
    task_ids_filter = or_(
        TaskInstance.task_id == 'get_ecs_configs',
        TaskInstance.task_id == 'step1_run_fargate',
        TaskInstance.task_id == 'get_emr_configs',
        TaskInstance.task_id == 'step2_start_EMR',
        TaskInstance.task_id == 'add_steps',
        TaskInstance.task_id == 'copy_lookup_tables',
        TaskInstance.task_id == 'run_src_etl',
        TaskInstance.task_id == 'run_composite_src_etl'
    )

    QQ = (
        session.query(TaskInstance).filter(
            TaskInstance.state == State.RUNNING,
            TaskInstance.dag_id == dag_id,
            task_ids_filter
        ))
    # step_running_tasks = QQ.all()                             # List of TaskInstance or []
    step_running_tasks = [QQ.first()] if QQ.first() else []     # String of the TaskInstance or None

    return step_running_tasks


def check_running(ti, **context):
    """
    Dag won't move forward to the EMR step if dag_id = 'cwr-two_steps' is
    :return:  'steps_running_STOP' or  'nothing_running_continue'
    """
    dag_id = ti.dag_id.lower()
    print(f"Checking tasks for DAG ID = {dag_id}")
    running_step_task_instances: tp.List[TaskInstance] = get_running_step_task_instances(dag_id)
    # extract relevant bits of info from TaskInstance(s) list (to serialize them)
    step_task_instances = list(map(ti_to_tuple, running_step_task_instances))
    for running_instances in step_task_instances:
        print(f"running_instances -> {running_instances}")

    return 'steps_running_STOP' if step_task_instances else 'nothing_running_continue'


# #################### Extending Operators Functionalities #####################
# Adding templated fields
# class MyECSOperator(ECSOperator):
#     template_fields = ECSOperator.template_fields + ('cluster',)


def testing_func(**context):
    ov_ecs = context['overridesECS']
    print(f"\t<<< overrides ECS TYPE:--{type(ov_ecs)}-- >>>")
    print(f"\t<<< overrides ECS:--{ov_ecs}-- >>>")

    ov_emr = context['overridesEMR']
    print(f"\t<<< overrides EMR TYPE:--{type(ov_emr)}-- >>>")
    print(f"\t<<< overrides EMR:--{ov_emr}-- >>>")

    var = context['ti'].xcom_pull(task_ids='get_ecs_configs', key='cluster_name')
    print_msg(f"\t<<< overrides ECS TYPE:--{type(var)}-- >>>")
    print_msg(f"\t<<< overrides ECS:--{var}-- >>>")


# ########################################## SLACK ###########################################
def slack_info(context):
    """
    Info could be:
        * Starting
        * Success finished
    """
    BUCKET = context['dag_run'].conf.get('BUCKET')
    OBJECT_KEY = context['dag_run'].conf.get('OBJECT_KEY')
    try:
        parser = AwsOps(BUCKET, OBJECT_KEY)
        file = parser.url
    except Exception as e:
        print(f"ERROR - Can't parse {BUCKET} - {OBJECT_KEY}: {e}")
        file = None

    # Message Producer Request Variables
    data_provider = context["dag_run"].conf.get("data_provider")
    data_provider_id = context["dag_run"].conf.get("data_provider_id")
    environment = context["dag_run"].conf.get("environment")
    file_standard = context["dag_run"].conf.get("file_standard")
    message_type = context["dag_run"].conf.get("message_type")
    timestamp_run = context["dag_run"].conf.get("timestamp_run")
    dry_run = context["dag_run"].conf.get("dry_run")

    ti = context.get('task_instance')
    task = ti.task_id
    dag = ti.dag_id
    # state = ti.state
    exec_date = context.get('execution_date')
    # log_url = ti.log_url

    if 'start' in task:
        if (BUCKET is not None) and (OBJECT_KEY is not None):
            slack_msg = f"""
                    :information_source: Running Airflow Pipeline for *Dag '{dag}'*
                    * *Processing file*: {file}
                    * *Execution Time*: {exec_date}
                    """
        elif (data_provider is not None) and (data_provider_id is not None) and (environment is not None):
            slack_msg = f"""
                    :information_source: Running Airflow Pipeline for *Dag '{dag}'*
                    * *Data Provider*: {data_provider}
                    * *Data Provider Id*: {data_provider_id}
                    * *Environment*: {environment}
                    * *File Standard*: {file_standard}
                    * *Request Type*: {message_type}"""
            if timestamp_run is not None:
                slack_msg += f"""
                    * *Timestamp Run*: {timestamp_run}"""
            slack_msg += f"""
                    * *Dry Run*: {dry_run}
                    * *Execution Time*: {exec_date}
                    """
        else:
            slack_msg = f"""
                    :information_source: Running Airflow Pipeline for *Dag '{dag}'*
                    * *Execution Time*: {exec_date}
                    """
    elif 'finish' in task:
        if (BUCKET is not None) and (OBJECT_KEY is not None):
            slack_msg = f"""
                         :white_check_mark: Airflow Pipeline for *Dag '{dag}'* successfully finished
                         * *Processed file*: {file}
                         * *Execution Time*: {exec_date}
                        """
        elif (data_provider is not None) and (data_provider_id is not None) and (environment is not None):
            slack_msg = f"""
                        :white_check_mark: Airflow Pipeline for *Dag '{dag}'* successfully finished
                        * *Data Provider*: {data_provider}
                        * *Data Provider Id*: {data_provider_id}
                        * *Environment*: {environment}
                        * *File Standard*: {file_standard}
                        * *Request Type*: {message_type}"""
            if timestamp_run is not None:
                slack_msg += f"""
                        * *Timestamp Run*: {timestamp_run}"""
            slack_msg += f"""
                        * *Dry Run*: {dry_run}
                        * *Execution Time*: {exec_date}
                        Was sent to scrap.
                        """
        else:
            slack_msg = f"""
                        :white_check_mark: Airflow Pipeline for *Dag '{dag}'* successfully finished
                        """
    else:
        if (BUCKET is not None) and (OBJECT_KEY is not None):
            slack_msg = f"""
                       :information_source: Airflow Pipeline. 
                       * *Dag '{dag}'*
                       * *Processing file*: {file}
                       * *Execution Time*: {exec_date}
                       """
        else:
            slack_msg = f"""
                       :information_source: Airflow Pipeline. 
                       * *Dag '{dag}'*
                       * *Execution Time*: {exec_date}
                       """

    slack_notification = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_conn',
        message=slack_msg,
        channel=SLACK_CHANNEL,
    )

    return slack_notification.execute(context=context)


def slack_failed(context):
    """
    SlackWebhookOperator - Connection:
            Host: https://hooks.slack.com/services
            Password: /aaa/bbb/ccc
    """
    BUCKET = context['dag_run'].conf.get('BUCKET')
    OBJECT_KEY = context['dag_run'].conf.get('OBJECT_KEY')

    try:
        parser = AwsOps(BUCKET, OBJECT_KEY)
        file = parser.url
    except Exception as e:
        print(f"ERROR - Can't parse {BUCKET} - {OBJECT_KEY}: {e}")
        file = None

    # Message Producer Request Variables
    data_provider = context["dag_run"].conf.get("data_provider")
    data_provider_id = context["dag_run"].conf.get("data_provider_id")
    environment = context["dag_run"].conf.get("environment")
    file_standard = context["dag_run"].conf.get("file_standard")
    message_type = context["dag_run"].conf.get("message_type")
    timestamp_run = context["dag_run"].conf.get("timestamp_run")
    dry_run = context["dag_run"].conf.get("dry_run")

    task = context.get('task_instance').task_id
    dag = context.get('task_instance').dag_id
    # ti = context.get('task_instance')
    exec_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    if (BUCKET is not None) and (OBJECT_KEY is not None):
        slack_msg = f"""
                :red_circle: Task Failed: couldn't process {file}
                * *Task*: {task}  
                * *Dag*: {dag} 
                * *Execution Time*: {exec_date}  
                * Please check _<{log_url}|*Log Url*>_ for more information.
                """
    elif (data_provider is not None) and (data_provider_id is not None) and (environment is not None):
        slack_msg = f"""
                :red_circle: Task Failed: couldn't process scrap request
                * *Task*: {task}  
                * *Dag*: {dag} 
                * *Data Provider*: {data_provider}
                * *Data Provider Id*: {data_provider_id}
                * *Environment*: {environment}
                * *File Standard*: {file_standard}
                * *Request Type*: {message_type}"""
        if timestamp_run is not None:
            slack_msg += f"""
                * *Timestamp Run*: {timestamp_run}"""
        slack_msg += f"""
                * *Dry Run*: {dry_run}
                * *Execution Time*: {exec_date}  
                * Please check _<{log_url}|*Log Url*>_ for more information.
                """
    else:
        slack_msg = f"""
                :red_circle: Task Failed: couldn't run ECS Task
                * *Task*: {task}  
                * *Dag*: {dag} 
                * *Execution Time*: {exec_date}  
                * Please check _<{log_url}|*Log Url*>_ for more information.
                """

    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_conn',
        message=slack_msg,
        channel=SLACK_CHANNEL
    )

    return failed_alert.execute(context=context)
