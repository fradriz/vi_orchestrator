from enum import Enum


class Standard:
    CWR = "cwr"
    BWARM = "bwarm"
    DDEX = "ddex"
    VI = "vi"
    JSON = "json"
    THIRD_PARTY = 'third_party'
    UI = 'ui'
    CWR_STEP2 = 'cwr_step2'

class Entity:
    WORK = 'work'
    RECORDING = 'recording'
    PARTICIPANT = 'participant'
    PRODUCT = 'product'

class Scrapers:
    DEEZER = 'deezer'
    IFPI = 'ifpi'
    ISWC = 'iswc'
    SONGVIEW = 'songview'

class StandardFileExtension:
    V21 = "v21"
    BZ2 = 'bz2'
    LOCK = 'lock'
    XML = "xml"
    JSON = "json"
    XLSX = "xlsx"
    CSV = 'csv'
    SC = "sc"


class ExecutionEnvironment:
    DEV = "development"
    STG = 'staging'
    PRD = "production"


class AwsLaunchType(Enum):
    Fargate = "fargate"
    EMR = "emr"


class EMRClusterSize:
    Small = "small"
    Medium = "medium"
    Large = "large"
    XLarge = "xlarge"


class ExecutionType:
    fargate = 'fargate'
    fargateEMR = 'fargateEMR'
    EMR = 'EMR'
