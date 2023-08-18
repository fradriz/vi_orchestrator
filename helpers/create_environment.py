import boto3


def create_mwaa_environment(aws_env='development'):
    """
    Create the Airflow environment
    :param aws_env: development | staging
    :return:

    Links:
        boto3: https://boto3.amazonaws.com/v1/documentation/api/1.18.43/reference/services/mwaa.html
        aws mwaa: https://awscli.amazonaws.com/v2/documentation/api/latest/reference/mwaa/create-environment.html
    """
    client = boto3.client('mwaa')

    if aws_env == 'development':
        account_id = 902980280986
    elif aws_env == 'staging':
        account_id = 664940443159
    else:
        raise Exception(f"Not a valid environment = '{aws_env}'")

    mwaa_env_name = f'orchestrator-DEV-{aws_env}'

    response = client.create_environment(
        Name=mwaa_env_name,
        # Environment variables. Read it: os.environ['AIRFLOW__ENVIRONMENT']
        AirflowConfigurationOptions={
            'core.environment': aws_env,
            # 'core.network.subnet': etl_fargate_configs['SubnetId'],
            # 'core.fargate.securityGroups': etl_fargate_configs['SecurityGroups'],
            # 'core.emr.bootstrapFile': source_etl_emr_configs['bootstrapFile'],
            'core.lazy_load_plugins': 'False',
            'smtp.smtp_port': '587',
            'smtp.smtp_ssl': 'false',
            'smtp.smtp_starttls': 'false',
            # 'smtp.smtp_mail_from': '---',
            # 'smtp.smtp_user': '---',
            # 'smtp.smtp_password': '---'
        },
        SourceBucketArn=f'arn:aws:s3:::vm-data-provisioning-{aws_env}',
        DagS3Path='airflow/dags',
        # PluginsS3ObjectVersion='1.0',
        # RequirementsS3ObjectVersion='1.0',
        PluginsS3Path='airflow/plugins/plugins.zip',
        RequirementsS3Path='airflow/requirements/requirements.txt',
        AirflowVersion='2.2.2',
        EnvironmentClass='mw1.small',
        ExecutionRoleArn=f'arn:aws:iam::{account_id}:role/service-role/AmazonMWAA-orchestrator-{aws_env}-cyTdiS',
        # KmsKey='string',
        LoggingConfiguration={
            'DagProcessingLogs': {
                'Enabled': True,
                'LogLevel': 'WARNING'
            },
            'SchedulerLogs': {
                'Enabled': True,
                'LogLevel': 'WARNING'
            },
            'TaskLogs': {
                'Enabled': True,
                'LogLevel': 'INFO'
            },
            'WebserverLogs': {
                'Enabled': True,
                'LogLevel': 'WARNING'
            },
            'WorkerLogs': {
                'Enabled': True,
                'LogLevel': 'WARNING'
            }
        },
        MaxWorkers=5,
        MinWorkers=1,
        NetworkConfiguration={
            'SecurityGroupIds': [
                'sg-09924201c10abbcc7',
            ],
            'SubnetIds': [
                'subnet-0498324ebac362cbb',
                'subnet-04ec89ca7b80100b0'
            ]
        },
        Schedulers=2,
        Tags={
            'team': 'data_engineer'
        },
        WebserverAccessMode='PUBLIC_ONLY',
        WeeklyMaintenanceWindowStart='TUE:03:30'
    )

    return response


def main():
    # resp = create_mwaa_environment(aws_env='staging')
    resp = create_mwaa_environment(aws_env='development')
    print(f"Response -> '{resp}'")


if __name__ == '__main__':
    main()
