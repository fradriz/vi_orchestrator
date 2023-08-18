#!/usr/bin/python3

import typing as tp
from etl.configs.clusters import ETL_FARGATE_CONFIGS, SOURCE_ETL_EMR_CONFIGS, ETL_FARGATE_CONFIGS_DDEX_PREPRO


class AWSDagParams:
    """
    Class get Dag parameters
    """

    def __init__(self, environment: str, execution_type: str = 'standard'):
        self.environment = environment
        self.fargate_configs = ETL_FARGATE_CONFIGS[environment]

        self.source_ecs_configs = self.fargate_configs['source']
        self.comp_source_ecs_configs = self.fargate_configs.get('composite-source')
        self.message_producer_ecs_configs = self.fargate_configs.get('message-producer')
        self.source_emr_configs = SOURCE_ETL_EMR_CONFIGS[environment]

    @property
    def ecs_network_subnets(self) -> tp.List:
        """
        Returning ECS Fargate subnets
        """
        return self.fargate_configs['SubnetId']

    @property
    def ecs_security_group(self) -> tp.List:
        """
        Returning ECS Fargate security group
        """
        return self.fargate_configs['SecurityGroups']

    # Source ECS Fargate configs
    @property
    def src_ecs_task_definition(self) -> str:
        return self.source_ecs_configs['taskDefinition']

    @property
    def src_ecs_cluster_name(self) -> str:
        return self.source_ecs_configs['cluster']

    @property
    def src_ecs_container_name(self) -> str:
        return self.source_ecs_configs['containerOverrides']['name']

    # Composite Source Configs
    @property
    def comp_src_ecs_task_definition(self) -> str:
        return self.comp_source_ecs_configs['taskDefinition']

    @property
    def comp_src_ecs_cluster_name(self) -> str:
        return self.comp_source_ecs_configs['cluster']

    @property
    def comp_src_ecs_container_name(self) -> str:
        return self.comp_source_ecs_configs['containerOverrides']['name']

    # Message Producer Configs
    @property
    def message_producer_ecs_task_definition(self) -> str:
        return self.message_producer_ecs_configs["taskDefinition"]

    @property
    def message_producer_ecs_cluster_name(self) -> str:
        return self.message_producer_ecs_configs["cluster"]

    @property
    def message_producer_ecs_container_name(self) -> str:
        return self.message_producer_ecs_configs["containerOverrides"]["name"]

    # EMR Configs
    @property
    def emr_bootstrap_file(self) -> str:
        return self.source_emr_configs['bootstrapFile']

    @property
    def emr_log_uri(self) -> str:
        return self.source_emr_configs['logUri']
