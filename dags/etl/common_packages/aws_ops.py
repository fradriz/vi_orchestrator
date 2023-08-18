#!/usr/bin/python3

import typing as tp
from etl.configs.enums import ExecutionEnvironment, Standard
from airflow.models import Variable


class AwsOps:
    """
    Class to work with AWS operations
    """

    def __init__(self, bucket: str, key: str):

        self.bucket = bucket
        self.key = key[1::] if key[0] == '/' else key  # Getting rid of the '/' character if it is received
        self.file_format = "parquet"
        # self.s3 = boto3.resource("s3")

    @property
    def url(self):
        """
        input: 'my_bucket/path/to/file.txt'
        url -> 's3://my_bucket/path/to/file.txt'
        """
        url = self.bucket + '/' + self.key
        return f"s3://{url.replace('//', '/')}"

    @property
    def execution_environment(self) -> ExecutionEnvironment:
        """
        | S3 bucket name with the following pattern: '<<clientName>>-vi-source-files[-<<environment>>]'.
        |
        | Returns ExecutionEnvironment enum value.
        |
        :returns: :class:ExecutionEnvironment
        """
        return Variable.get('ENVIRONMENT')

    @property
    def dataProvider(self) -> str:
        """
        | S3 bucket name with the following pattern: '<dataProviderName>-<<dataProviderId>>-source-<<env>>'
        :returns: :str:dataProvider name.
        """
        if self.standard == Standard.VI:
            return self.key.split('/')[1].split('-')[0]
        else:
            return self.bucket.split('-')[0]

    @property
    def dataProviderId(self) -> tp.Optional[str]:
        """
        | TBO S3 Keys follows the following pattern: '<<dataProviderId>>/<<standard>>/<<fileToProcess>>'.
        |
        | Returns a dataProviderId value.
        |
        :returns: str
        """
        if self.standard == Standard.VI:
            return self.key.split('/')[1].split('-')[1]
        else:
            return self.bucket.split('-')[1]

    @property
    def standard(self) -> str:
        """
        | S3 Keys follows the following pattern: '<<dataProviderId>>/<<standard>>/<<fileToProcess>>'.
        | Returns Standard enum value.
        |
        :returns: :str: file type standard (cwr, ddex, bwarm, etc)
        """
        if 'xlsx' in self.key:
            return self.key.split("/")[2].lower()
        else:
            return self.key.split("/")[1].lower()

    @property
    def data_lake_bucket(self):
        return f"{self.dataProvider}-{self.dataProviderId}-data-lake-{self.execution_environment}"

    @property
    def preProdKey(self) -> str:
        return f"{self.dataProviderId}/preprod_data/works/"
