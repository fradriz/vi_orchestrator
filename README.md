# Orchestrator (Airflow) repository

*Airflow* can be used as an orchestrator by defining workflows as Directed Acyclic Graphs (*DAG*s) of tasks, where the *task*s are atomic units of work run by *Executors*. 
*Tasks* can be scheduled to run periodically or triggered by external events, and dependencies between tasks can be defined using the *DAG* structure. 
*Airflow* also provides a web interface for managing and monitoring workflows, as well as a rich API for programmatic interaction with the platform.

## CI/CD
CodeBuild actions has been defined at the file `.aws/buildspec.yml` and is linked to GitHub using a WebHook.

<font color=red>__WARNING__: In each new merge to both `development` or `staging`, data stored at `s3://vm-data-provisioning-$ENV/airflow/dags/etl/` will be overwritten ! </font>


## Folder structure. 
Use directories under *DAGs* root folder, according to the official Airflow [documentation instructions](https://airflow.apache.org/docs/apache-airflow/stable/modules_management.html#typical-structure-of-packages).

*DAG*s base folder: `s3://vm-data-provisioning-$ENV/airflow/dags/` where `ENV` could be `development | staging`

This is a possible structure to follow: 
```
airflow
├── dags
│   ├── etl
│   │   ├── 3rd_party_dags
│   │   ├── common_packages
│   │   │   ├── aws_ops.py
│   │   │   ├── cls_params.py
│   │   │   └── functions.py
│   │   ├── configs
│   │   │   ├── clusters.py
│   │   │   └── enums.py
│   │   ├── cwr_dags
│   │   │   ├── dynamic_dags
│   │   │   │   ├── cwr-big_dag_un.py
│   │   │   │   └── cwr-big_dag_wcm.py
│   │   │   └── medium.py
│   │   ├── ddex_dags
│   │   ├── dynamic_dags_builder
│   │   │   └── cwr
│   │   │       ├── cwr_big_dag_builder.py
│   │   │       └── templates
│   │   │           └── big-dag-template.py
│   │   ├── testing_dags
│   │   │   ├── test_dynamic_create_dags.py
│   │   │   └── testing_dag.py
│   │   └── vi_dags
│   └── other_testing_dags
└── plugins
       └── plugins.zip
```
Where the `etl` folder will hold the etl dags.
In case we need to test some dags or use them for something different such as a merge, we can create new folders (eg `test_merge`)

_IMPORTANT_: The `.airflowignore` file tells the *Scheduler* to ignore certain source files that are not *DAG*s. 
This file content list path prefixes, like: 
```
etl/configs/.*
etl/common_packages/.*
```
## Deploying Dags
First update the `.airflowignore` file and then upload the `etl` directory to s3:
```shell
$ aws s3 cp .airflowignore s3://vm-data-provisioning-development/airflow/dags/.airflowignore
$ aws s3 cp etl s3://vm-data-provisioning-development/airflow/dags/etl/ --recursive

# ... or just update a single dag if required
$ aws s3 cp ecs_fargate.py s3://vm-data-provisioning-development/airflow/dags/etl/cwr_dags/ecs_fargate.py
```

## Best Practices
Highlights extracted from [this guide](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

* _Idempotency_. Tasks should produce the same outcome on every re-run. Some of the ways you can avoid producing a different result:
    * Do not use INSERT during a task re-run, an INSERT statement might lead to duplicate rows in your database. Replace it with UPSERT.

    * Read and write in a specific partition.

    * The Python datetime now() function gives the current datetime object. This function should never be used inside a task, especially to do the critical computation, as it leads to different outcomes on each run. It’s fine to use it, for example, to generate a temporary log.
  

* _Deleting a task_. Be careful when deleting a task from a DAG. You would not be able to see the Task in Graph View, Tree View, etc making it difficult to check the logs of that Task from the Webserver. If that is not desired, please create a new DAG.


* _Communication_. 
  * you should not store any file or config in the local filesystem as the next task is likely to run on a different server without access to it.
  * Use `XCOM` to communicate small messages between tasks and a good way of passing larger data between tasks is to use a remote storage such as S3/HDFS.
  * <font color=red>The tasks __should not store any authentication parameters__ such as passwords or token inside them.</font> Use the [AWS secrets manager](https://docs.aws.amazon.com/mwaa/latest/userguide/connections-secrets-manager.html) instead.
  
* _Top level Python Code_. 
  * You should not run any database access, heavy computations and networking operations outside the Operator’s `execute methods.
  * Top-level imports might take surprisingly a lot of time and they can generate a lot of overhead and _this can be easily avoided by converting them to local imports inside Python callables_.
  
* _Triggering DAGs after changes_. Avoid triggering DAGs immediately after changing them or any other accompanying files that you change in the DAG folder.

## Extending Operators Functionalities
Adding *Templated Fields*
```python
class MyECSOperator(ECSOperator):
  template_fields = ECSOperator.template_fields + ('cluster',)
```

## [Dynamic Dags](https://docs.astronomer.io/learn/dynamically-generating-dags)
Sometimes it is useful to work with dynamic _DAG_s, especially when the same *DAG* behaviour needs to be repeated over and over.

There are different options to achieve this, each having its pros and cons. We suggest using the [multiple files method](https://docs.astronomer.io/learn/dynamically-generating-dags#multiple-file-methods)

This project includes an example of this kind. To test it run the following command:
```shell
$ cd orchestrator/
$ PYTHONPATH=<full path>/orchestrator/dags/ python dags/etl/dynamic_dags_builder/cwr/cwr_big_dag_builder.py development

Environment == development
Data providers to create DAGs:['wcm', 'un']
Creating a new file from template 'dags/etl/dynamic_dags_builder/cwr/templates/big-dag-template.py' -->> 'dags/etl/cwr_dags/dynamic_dags/cwr-big_dag_wcm.py'
Creating a new file from template 'dags/etl/dynamic_dags_builder/cwr/templates/big-dag-template.py' -->> 'dags/etl/cwr_dags/dynamic_dags/cwr-big_dag_un.py'
```
When calling the `cwr_big_dag_builder.py` file it is required to pass the environment value as an argument.

As the output highlights, it will create two different dags for both data providers.

## Local environment to test the Dags
Current *Airflow* version implemented at *MWAA* is 2.2.2 (latest nowadays).

To check operators (MWAA is currently using _ECSOperator_ instead of the newer _EcsOperator_) and libraries compatibilities is suggested to have a local virtual environment with the right version.

So, for Airflow 2.2.2 ...
```shell
$ virtualenv mwaa_env
$ pip install apache-airflow==2.2.2
$ pip install apache-airflow-providers-amazon==3.4.0
```

Following [this article](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#triggering-dags-after-changes), it is possible to test the created *DAG*s:
```shell
# send the PYTHONPATH and then run the dag
$ PYTHONPATH=<full path>/orchestrator/dags/ python dags/etl/testing_dags/testing_dag.py
```
Links:
* https://pypi.org/project/apache-airflow-providers-amazon/3.4.0/
* https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/index.html
