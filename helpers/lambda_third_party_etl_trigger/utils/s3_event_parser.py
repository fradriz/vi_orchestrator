import typing as tp
from datetime import datetime
from configs.enums import ExecutionEnvironment


class S3EventParser:
    def __init__(self, event: tp.Dict[str, tp.Any]) -> None:
        self._event = event

    @property
    def record(self) -> tp.Dict[str, tp.Any]:
        return self._event["Records"][0]["s3"]

    @property
    def inputBucketName(self) -> str:
        return self.record["bucket"]["name"]

    @property
    def objectKey(self) -> str:
        return self.record["object"]["key"].replace("%3D", "=")

    @property
    def dataProvider(self) -> str:
        """
        | S3 bucket name with the following pattern: '<dataProviderName>-<<dataProviderId>>-source-<<env>>'
        :returns: :str:dataProvider name.
        """
        return self.inputBucketName.split('-')[0]

    @property
    def dataProviderId(self) -> tp.Optional[str]:
        """
        | TBO S3 Keys follows the following pattern: '<<dataProviderId>>/<<standard>>/<<fileToProcess>>'.
        |
        | Returns a dataProviderId value.
        |
        :returns: str
        """
        return self.inputBucketName.split('-')[1]

    @property
    def executionEnvironment(self) -> ExecutionEnvironment:
        """
        | S3 bucket name with the following pattern: '<<clientName>>-vi-source-files[-<<environment>>]'.
        |
        | Returns ExecutionEnvironment enum value.
        |
        :returns: :class:ExecutionEnvironment
        """
        bck = self.inputBucketName.lower()

        if ("-dev" in bck) or ("-development" in bck):
            return ExecutionEnvironment.DEV
        elif ("-stg" in bck) or ("-staging" in bck):
            return ExecutionEnvironment.STG
        else:
            return ExecutionEnvironment.PRD

    @property
    def bucketSuffix(self) -> str:
        """
        | If input bucket ends with '-dev', then output and exports should use the same
        |
        :returns: :str: bucket suffix (dev, development, stg, staging, prd, production)
        """
        return self.inputBucketName.lower().split('-')[-1]


    @property
    def standard(self) -> str:
        """
        | S3 Keys follows the following pattern: '<<dataProviderId>>/<<standard>>/<<fileToProcess>>'.
        | Returns Standard enum value.
        |
        :returns: :str: file type standard (cwr, ddex, bwarm, etc)
        """
        return st.CWR if self.inputFileExtension == StandardFileExtension.LOCK else self.objectKey.split("/")[1].lower()

    @property
    def inputFileExtension(self) -> str:
        """
        |
        :returns: :str:File extension
        """
        return self.objectKey.split(".")[-1].lower()

    @property
    def inputFileName(self) -> str:
        return self.objectKey.split("/")[-1].lower()

    @property
    def outputBucket(self) -> str:
        return f"{self.dataProvider}-{self.dataProviderId}-data-lake-{self.bucketSuffix}"

    @property
    def exportBucket(self) -> str:
        return f"{self.dataProvider}-{self.dataProviderId}-exports-{self.bucketSuffix}"

    @property
    def inputFileURI(self) -> str:
        return f"s3://{self.inputBucketName}/{self.objectKey}"

    @property
    def outputBucketURI(self) -> str:
        return f"s3://{self.outputBucket}/"

    @property
    def exportBucketURI(self) -> str:
        return f"s3://{self.exportBucket}/"

    def lockKey(self, step: int) -> str:
        if step == 1:
            return f'control_state/running/{self.dataProvider.upper()}_step1_{self.inputFileName}.lock'
        else:
            # step 2
            now = str(datetime.utcnow().strftime("%Y%m%d_%H%M%S"))
            return f'control_state/running/step2_{now}.lock'

    @property
    def preProdKey(self) -> str:
        return f"{self.dataProviderId}/preprod_data/works/"
