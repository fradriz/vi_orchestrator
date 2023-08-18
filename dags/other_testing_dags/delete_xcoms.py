import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG

# https://stackoverflow.com/questions/46707132/how-to-delete-xcom-objects-once-the-dag-finishes-its-run-in-airflow

from airflow.models import DAG
from airflow.utils.db import provide_session
from airflow.models import XCom
from datetime import datetime, timedelta, timezone


@provide_session
def cleanup_xcom(context, session=None):
    dag_id = context["ti"]["dag_id"]
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


@provide_session
def cleanup_xcom(session=None, **context):
    # dagg = context["dag"]
    # dag_id = dagg._dag_id
    dag_id = 'dag_I_want_to_delete_data_from'
    # It will delete all xcom of the dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


# Esto me gustó !
@provide_session
def cleanup_xcom(session=None):
    ts_limit = datetime.now(timezone.utc) - timedelta(days=1)
    session.query(XCom).filter(XCom.execution_date <= ts_limit).delete()
    print(f"deleted all XCOMs older than {ts_limit}")


with DAG(
        dag_id='delete_old_xcoms',
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        render_template_as_native_obj=True,
        catchup=False,
        on_success_callback = cleanup_xcom,
) as dag:
    # cleanup_xcom
    xcom_cleaner = PythonOperator(
        task_id='delete-old-xcoms',
        python_callable=cleanup_xcom)

    clean_xcom = PythonOperator(
        task_id="clean_xcom",
        python_callable = cleanup_xcom,
        # provide_context=True,
        # dag=dag
    )

# Using the PostgresOperator ?
    delete_xcom_task = PostgresOperator(
          task_id='delete-xcom-task',
          postgres_conn_id='airflow_db',
          sql="delete from xcom where dag_id=dag.dag_id and task_id='your_task_id' and execution_date={{ ds }}",
          dag=dag)

    delete_xcom_task_inst = PostgresOperator(
        task_id='delete_xcom',
        postgres_conn_id='your_conn_id',
        sql="delete from xcom where dag_id= '" + dag.dag_id + "' and date(execution_date)=date('{{ ds }}')"
    )
