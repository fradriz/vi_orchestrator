from etl.configs.enums import ExecutionType, EMRClusterSize
from etl.configs.enums import StandardFileExtension as fe
from etl.configs.enums import Standard as st

# ############################ FARGATE CONFIGS #############################
ETL_FARGATE_CONFIGS = {
    'development': {
        'source': {
            'cluster': 'cwr-etl-cluster',
            'taskDefinition': 'cwr-job-task',
            'containerOverrides': {'name': 'cwr_etl'},
        },
        'composite-source': {
            'cluster': 'composite-etl',
            'taskDefinition': 'composite-etl',
            'containerOverrides': {'name': 'composite_etl_container'},
        },
        "message-producer": {"cluster": "cwr-etl-cluster",
                             "taskDefinition": "third-party-message-producer",
                             "containerOverrides": {'name': "ThirdPartyMessageProducer"}},
        'SubnetId': ['subnet-012fbd487500ee9cd', 'subnet-038a92a58a5e459a4'],
        'SecurityGroups': ["sg-0"]
    },
    'staging': {
        'source': {
            'cluster': 'staging-data-cluster',
            'taskDefinition': 'cwr_etl',
            'containerOverrides': {'name': 'cwr_etl'},
        },
        'composite-source': {
            'cluster': 'staging-composite-etl-cluster',
            'taskDefinition': 'composite-etl',
            'containerOverrides': {'name': 'composite-etl'},
        },
        "message-producer": {"cluster": "cwr-etl-cluster",
                             "taskDefinition": "third-party-message-producer",
                             "containerOverrides": {'name': "third-party-message-producer"}},
        'SubnetId': ['subnet-027f7cd0cb29ff82a', 'subnet-07f1604aa6460935e'],
        'SecurityGroups': ["sg-01508bb6d07d6409c"]
    },
    'production': {
        'source': {
            'cluster': 'TBD',
            'taskDefinition': 'TBD',
            'containerOverrides': {'name': 'TBD'},
        },
        'composite-source': {
            'cluster': 'TBD',
            'taskDefinition': 'TBD',
            'containerOverrides': {'name': 'TBD'},
        },
        "message-producer": {"cluster": "TBD",
                             "taskDefinition": "TBD",
                             "containerOverrides": {'name': "TBD"}},
        'SubnetId': ['TBD'],
        'SecurityGroups': ["TBD"]
    }
}

ETL_FARGATE_CONFIGS_DDEX_PREPRO = {
    'development': {
        'source': {
            'cluster': 'ddex-preprocessing',
            'taskDefinition': 'ddex-preprocessing',
            'containerOverrides': {'name': 'ddex-preprocessing'},
        },
        'SubnetId': ['subnet-012fbd487500ee9cd', 'subnet-038a92a58a5e459a4'],
        'SecurityGroups': ["sg-0"]
    },
    'staging': {
        'source': {
            'cluster': 'staging-data-cluster',
            'taskDefinition': 'ddex-preprocessing',
            'containerOverrides': {'name': 'ddex-preprocessing'},
        },
        'SubnetId': ['subnet-027f7cd0cb29ff82a', 'subnet-07f1604aa6460935e'],
        'SecurityGroups': ["sg-01508bb6d07d6409c"]
    }
}


SOURCE_FARGATE_OVERRIDES_BASE = {
            'containerOverrides': [
                {
                    'name': '$container_name',
                    'command': ['--verbose', 'false',
                                '--lookup_local', 'true',
                                '--input_file', 's3://$BUCKET/$OBJECT_KEY',
                                '--output_path', 's3://$DATA_PROVIDER-$DATA_PROVIDER_ID-data-lake-$ENV/',
                                '--export_path', 's3://$DATA_PROVIDER-$DATA_PROVIDER_ID-exports-$ENV/',
                                '--file_type', '$FILE_TYPE',
                                '--data_provider_id', '$DATA_PROVIDER_ID',
                                '--steps', '$ETL_STEPS'
                                ]
                }]}

DDEX_FARGATE_OVERRIDES_BASE = {
            'containerOverrides': [
                {
                    'name': '$container_name',
                    'command': ['--verbose', 'false',
                                '--lookup_local', 'true',
                                '--specification_file', 's3://$BUCKET/$OBJECT_KEY',
                                '--output_path', 's3://$DATA_PROVIDER-$DATA_PROVIDER_ID-data-lake-$ENV/',
                                '--export_path', 's3://$DATA_PROVIDER-$DATA_PROVIDER_ID-exports-$ENV/',
                                '--status_path', 's3://$DATA_PROVIDER-$DATA_PROVIDER_ID-source-$ENV/$DATA_PROVIDER_ID/launcher-state/',
                                '--file_type', '$FILE_TYPE',
                                '--data_provider_id', '$DATA_PROVIDER_ID'
                                ]
                }]}


DDEX_PREPRO_FARGATE_OVERRIDES_BASE = {
            'containerOverrides': [
                {
                    'name': '$container_name',
                    'command': ['--run_type', 'copy_files_to_vi',
                                '--dataProviderName', 'fuga',
                                '--dataProviderId', '187',
                                '--source_prefix', 'fuga/production',
                                '--sentOnBehalfOf', 'stmpd_rcrds',
                                '--SBH_ClientName', 'stmpdrcrds',
                                '--SBH_provider_id', '6'
                                ]
                }]}

SOURCE_FARGATE_THIRD_PARTY_OVERRIDES_BASE = {
    'containerOverrides': [
        {
            'name': '$container_name',
            'command': ['--verbose', 'false',
                        '--lookup_local', 'true',
                        '--input_file', 's3://$BUCKET/$OBJECT_KEY',
                        '--output_path', 's3://$DATA_PROVIDER-data-lake-$ENV/',
                        '--export_path', 's3://$DATA_PROVIDER-exports-$ENV/',
                        '--file_type', 'third_party',
                        '--tenant', '$TENANT'
                        ]
        }]}

COMPOSITE_SOURCE_FARGATE_OVERRIDES_BASE = {
    'containerOverrides': [
        {
            'name': '$container_name',
            'command': ['--verbose', 'false',
                        '--dataLakePath',
                        's3://$DATA_PROVIDER-$DATA_PROVIDER_ID-data-lake-$ENV/$DATA_PROVIDER_ID/data_lake/',
                        '--output_path', "s3://composite-vi-source-$ENV/vi_media/data_lake/",
                        '--data_provider', '$DATA_PROVIDER-$DATA_PROVIDER_ID',
                        '--whole', 'true'
                        ]
        }]}

SOURCE_FARGATE_NETWORK_CONFIGS_BASE = {
    'awsvpcConfiguration': {
        'subnets': '$subnets',
        'securityGroups': '$security_groups',
        'assignPublicIp': 'ENABLED'
    }
}

MESSAGE_PRODUCER_FARGATE_OVERRIDES_BASE = {
    "containerOverrides": [{"name": "$CONTAINER_NAME",
                            "command": ["--fileStandard", "$FILE_STANDARD",
                                        "--dataLakePath", "s3a://$DATA_PROVIDER-$DATA_PROVIDER_ID-data-lake-$ENV/"
                                                          "$DATA_PROVIDER_ID/data_lake/",
                                        "--messageType", "$MESSAGE_TYPE",
                                        "--dryRun", "$DRY_RUN",
                                        "$TIMESTAMP_RUN_FLAG", "$TIMESTAMP_RUN_VALUE"]}]
}

# ############################## EMR CONFIGS ###############################
# 'SubnetId': SOURCE_ETL_FARGATE_CONFIGS['development']['SubnetId'],
SOURCE_ETL_EMR_CONFIGS = {
    'development': {
        'bootstrapFile': 's3://vm-data-provisioning-development/emr/bootstrap_actions/emr_bootstrap.sh',
        'logUri': 's3n://vm-data-provisioning-development/emr/logs/',
        'releaseLabel': 'emr-6.4.0',
        'security': {
            'Ec2SubnetId': 'subnet-038a92a58a5e459a4',
            'EmrManagedSlaveSecurityGroup': 'sg-0c5b27bef4573c164',
            'EmrManagedMasterSecurityGroup': 'sg-0c216a3bde6f64455'
        }
    },
    'staging': {
        'bootstrapFile': 's3://vm-data-provisioning-staging/emr/bootstrap_actions/emr_bootstrap.sh',
        'logUri': 's3n://vm-data-provisioning-staging/emr/logs/',
        'releaseLabel': 'emr-6.4.0',
        'security': {
            'Ec2SubnetId': 'subnet-05ecc97088a503943',
            'EmrManagedSlaveSecurityGroup': 'sg-06a98c29178eeff13',
            'EmrManagedMasterSecurityGroup': 'sg-014bb2b1035cf50a5'
        }
    },
    'production': {
        'bootstrapFile': 's3://vm-data-provisioning-production/emr/bootstrap_actions/emr_bootstrap.sh',
        'logUri': 's3n://vm-data-provisioning-production/emr/logs/',
        'releaseLabel': 'emr-6.4.0',
        'security': {
            'Ec2SubnetId': 'TBD',
            'EmrManagedSlaveSecurityGroup': 'TBD',
            'EmrManagedMasterSecurityGroup': 'TBD'
        }
    }
}

CLUSTER_SIZE_CONFIGS = {
    'small': dict(
        MasterInstanceType='m5.4xlarge',
        InstanceType='m5.4xlarge',
        InstanceCount=3,
    ),
    'medium': dict(
        MasterInstanceType='m5.8xlarge',
        InstanceType='m5.8xlarge',
        InstanceCount=4,
    ),
    'large': dict(
        MasterInstanceType='r5.12xlarge',
        InstanceType='r5.12xlarge',
        InstanceCount=3,
    )
}

JOB_FLOW_OVERRIDES_BASE = {
    'Name': '$Name',
    'LogUri': 's3://vm-data-provisioning-$env/emr/logs/',
    'ReleaseLabel': 'emr-6.4.0',
    'BootstrapActions': [{
        'Name': 'BootStrap - CWR ETL Libs',
        'ScriptBootstrapAction': {
            'Args': ['$env'],
            'Path': 's3://vm-data-provisioning-$env/airflow/dags/etl/configs/emr_bootstraps/bigdag_bootstrap.sh'
        }
    }],
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': '$InstanceType',
                'InstanceCount': 1,
            },
            {
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': '$InstanceType',
                'InstanceCount': '$InstanceCount',
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        # 'Ec2KeyName': 'mykeypair',
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole'
}

# $input_files = 210001, 210002, 210003, 210004, 210005
SPARK_STEPS_2_BASE = [
    {
        "Name": "Copy Lookup Tables to HDFS",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["bash", "-c", "hadoop fs -put /home/hadoop/cwr_etl/lookup_tables hdfs:///user/hadoop/"]
        },
    },
    {
        "Name": "Source ETL Job",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit",
                     "--master", "yarn", "--deploy-mode", "client", "--jars", "/home/hadoop/delta-core_2.12-1.0.0.jar",
                     "/home/hadoop/cwr_etl/main.py",
                     "--verbose", "false",
                     "--emr_cluster", "true",
                     "--cwr_params", "/home/hadoop/cwr_etl/configs/cwr_config.json",
                     "--file_type", "$file_type",
                     "--input_file", "$input_files",
                     "--output_path", "s3://$DATA_PROVIDER-$DATA_PROVIDER_ID-data-lake-$ENV/",
                     "--export_path", "s3://$DATA_PROVIDER-$DATA_PROVIDER_ID-exports-$ENV/",
                     "--data_provider_id", "$DATA_PROVIDER_ID",
                     "--lookup_local", "true",
                     "--steps", "2/2"]
        },
    },
    {
        "Name": "Composite-source ETL Job",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit",
                     "--master", "yarn", "--deploy-mode", "client", "--jars", "/home/hadoop/delta-core_2.12-1.0.0.jar",
                     "/home/hadoop/composite-etl/main.py",
                     "--verbose", "false",
                     "--emr_cluster", "true",
                     "--dataLakePath", "s3://$DATA_PROVIDER-$DATA_PROVIDER_ID-data-lake-$ENV/"
                                       "$DATA_PROVIDER_ID/data_lake/",
                     "--output_path", "s3://composite-vi-source-$ENV/vi_media/data_lake/",
                     "--whole", "true",
                     "--data_provider", "$DATA_PROVIDER-$DATA_PROVIDER_ID"
                     ]
        },
    },
]

DATA_PROVIDERS_CONF = {
    'default': {
        'execution_type': ExecutionType.fargate,
        'cluster_size': EMRClusterSize.Small,
        # 'base_args': ETL_BASE_ARGS['fargate']
    },

    'mlc': {
        'execution_type': ExecutionType.EMR,
        'cluster_size': EMRClusterSize.Large,
    },

    'wcm': {
        'execution_type': ExecutionType.fargateEMR,
        'cluster_size': EMRClusterSize.Medium
    },

    'un': {
        'execution_type': ExecutionType.fargateEMR,
        'cluster_size': EMRClusterSize.Small
    }
}

# assert ('V21' in DATA_EXTENSIONS['cwr'])
DATA_EXTENSIONS = {
    st.CWR: [fe.V21, fe.BZ2],
    st.CWR_STEP2: [fe.LOCK],
    st.UI: [fe.JSON],
    st.THIRD_PARTY: [fe.JSON],
    st.DDEX: [fe.XML],
    st.VI: [fe.XLSX, fe.CSV],
    st.BWARM: ['']
}


def get_task_definition(ecs, task_definition_name):
    """
    Getting the latest task definition in the environment.
    :param ecs: ecs boto3 connector
    :param task_definition_name: cwr-job-task
    :return: Latest task definition. i.e. 'cwr-job-task:3'
    """
    task_def_arn = ecs.describe_task_definition(taskDefinition=task_definition_name)['taskDefinition'][
        'taskDefinitionArn']
    return task_def_arn.split('/')[1]
