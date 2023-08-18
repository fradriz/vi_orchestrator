# Lambda: Airflow trigger
Useful to test the dags and to run them manually. It is not meant to use in production, just testing. 

AWS Lambda name: `airflow_trigger`

Lambda function used to trigger any Airflow dag.

## Deploy / Remove serverless
To deploy the trigger lambda in AWS just run:

Development environment
```shell
sls deploy --stage development --verbose
# sls remove --stage development --verbose
```


