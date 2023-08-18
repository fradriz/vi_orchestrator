from other_testing_dags.athena.athena_schemas.works import WORKS_SCHEMA

QUERY_CREATE_TABLE = """
CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE} 
( {SCHEMA} )
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://bls-3-data-lake-staging/3/data_lake/works/_symlink_format_manifest'
TBLPROPERTIES (
  'creator'='data_engineering')
"""

print(QUERY_CREATE_TABLE.format(ATHENA_DATABASE='bla', ATHENA_TABLE='tabla', SCHEMA=WORKS_SCHEMA))
