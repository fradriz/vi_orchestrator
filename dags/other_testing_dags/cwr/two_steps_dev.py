"""
Run Source ETL in two steps to process large amount of data.
This is used with data providers like WCM or Un
"""
import os
import time
import pendulum
from typing import List, Optional, Tuple
from sqlalchemy import or_

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable
from airflow.utils.state import State
from airflow.settings import Session
from airflow.utils.db import provide_session
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from etl.common_packages.functions import print_msg
from etl.common_packages.cls_params import AWSDagParams


def step1_fargate(**context):
    print("test")
    # VAR = os.environ
    # print(f"<<< {VAR} >>>")
    # print(f"<<< Running SOURCE FARGATE ETL for {context['dag_run'].conf.get('data_provider_name')} >>>")
    # time.sleep(10)
    # print(f"\t<<< dag_run:--{context['dag_run'].conf}-- >>>")


def step2_emr(**context):
    print(f"<<< Running STEP2 EMR - DATA PROVIDER:{context['dag_run'].conf.get('data_provider_name')} >>>")
    time.sleep(60)
    # print(f"\t<<< dag_run:--{context['dag_run'].conf}-- >>>")


def ti_to_tuple(ti: TaskInstance) -> Tuple:
    """
    Converts a TaskInstance in List[str] by extracting relevant bits of info from it
    :param ti: TaskInstance
    :return: List[str]
    """
    return ti.dag_id, ti.task_id, ti.state, ti.start_date


@provide_session
def get_running_step_task_instances(session: Optional[Session] = None) -> List[TaskInstance]:
    """
    https://stackoverflow.com/questions/63428936/how-to-get-the-list-of-all-the-failed-tasks-from-different-dags
    Returns list of running TaskInstance(s) for step 1
    :param session: Optional[Session]
    :return: List[TaskInstance]
    """
    task_ids_filter = or_(
        TaskInstance.task_id == 'pythonOP_step1_fargate',
        TaskInstance.task_id == 'PythonOP_step2_EMR')

    QQ = (
        session.query(TaskInstance).filter(
            TaskInstance.state == State.RUNNING,
            TaskInstance.dag_id == 'cwr_two_steps-DEV',
            task_ids_filter
            # TaskInstance.task_id == 'step1_fargate',
        ))

    # Showing the query
    from sqlalchemy.dialects import postgresql
    print_msg(str(QQ.statement.compile(dialect=postgresql.dialect())))

    # step_running_tasks = QQ.all()                             # List of TaskInstance or []
    step_running_tasks = [QQ.first()] if QQ.first() else []     # String of the TaskInstance or None

    print_msg(QQ.all())
    print_msg(QQ.first())

    return step_running_tasks


def check_running():
    """
    :return:  'still_running' or  'nothing_running'
    """
    running_step_task_instances: List[TaskInstance] = get_running_step_task_instances()
    # extract relevant bits of info from TaskInstance(s) list (to serialize them)
    step_task_instances = list(map(ti_to_tuple, running_step_task_instances))
    for running_instances in step_task_instances:
        print(f"running_instances -> {running_instances}")

    return 'still_running' if step_task_instances else 'nothing_running'


with DAG(
        dag_id="cwr_two_steps-DEV",
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
) as dag:
    ENVIRONMENT = Variable.get('ENVIRONMENT')
    params = AWSDagParams(ENVIRONMENT)

    # Step 1: Multiple ECS Fargate running in parallel
    pythonOP_step1_fargate = PythonOperator(
        task_id='pythonOP_step1_fargate',
        dag=dag,
        python_callable=step1_fargate,
        depends_on_past=False)

    # Branch: The next task depends on the return from the python function check_api
    check_running = BranchPythonOperator(
        task_id='check_running',
        # provide_context=True,
        python_callable=check_running
    )

    still_running = DummyOperator(
        task_id='still_running'
    )

    nothing_running = DummyOperator(
        task_id='nothing_running'
    )

    # Step 2: EMR
    PythonOP_step2_EMR = PythonOperator(
        task_id='PythonOP_step2_EMR',
        dag=dag,
        python_callable=step2_emr,
        depends_on_past=False)

    pythonOP_step1_fargate >> check_running >> still_running
    pythonOP_step1_fargate >> check_running >> nothing_running >> PythonOP_step2_EMR
