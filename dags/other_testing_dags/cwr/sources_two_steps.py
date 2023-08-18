import time

import pendulum

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from typing import List, Optional, Tuple
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State
from airflow.settings import Session
from airflow.utils.db import provide_session


def step1_fargate(**kwargs):
    print(f"<<< Running SOURCE FARGATE ETL for {kwargs['dag_run'].conf.get('data_provider_name')} >>>")
    time.sleep(10)
    print(f"\t<<< dag_run:--{kwargs['dag_run'].conf}-- >>>")
    print(f"\t<<< ARGS1:--{kwargs['name']}-- >>>")

    # print(f"\t<<< dag_run.conf:--{kwargs['dag_run'].conf.get('data_provider_name')}-- >>>")
    # raise ValueError('This will turns the python task in failed state')
    # return 0


def ti_to_string(ti: TaskInstance) -> List[str]:
    """
    Converts a TaskInstance in List[str] by extracting relevant bits of info from it
    :param ti: TaskInstance
    :return: List[str]
    """
    return [ti.dag_id, ti.task_id, ti.state, ti.start_date]


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
    Returns list of failed TaskInstance(s)
     - for all DAGs since inception of time
     - sorted by (1) dag_id ASC (2) start_date DESC
    :param session: Optional[Session]
    :return: List[TaskInstance]
    """
    step_running_tasks = (
        session.query(TaskInstance)
            .filter(TaskInstance.state == State.RUNNING,
                    TaskInstance.dag_id == 'two_steps_POC',
                    TaskInstance.task_id == 'step1_fargate',
                    )
            .all()
    )
    return step_running_tasks


def check_running():
    """

    :return:  'still_running' or  'nothing_running'
    """
    running_step_task_instances: List[TaskInstance] = get_running_step_task_instances()
    # extract relevant bits of info from TaskInstance(s) list (to serialize them)
    # step_task_instances: List[List[str]] = list(map(ti_to_string, running_step_task_instances))
    step_task_instances = list(map(ti_to_tuple, running_step_task_instances))
    for running_instances in step_task_instances:
        print(f"running_instances -> {running_instances}")

    status = 'still_running' if step_task_instances else 'nothing_running'
    print(f"current status: '{status}'")

    return status


with DAG(
        dag_id="two_steps_POC",
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
) as dag:
    # Step 1: Multiple ECS Fargates running in parallel
    step1_fargate = PythonOperator(
        task_id='step1_fargate_DEV',
        dag=dag,
        python_callable=step1_fargate,
        op_kwargs={'name': DATA_PROVIDER, 'TASK': f'-->{task_definition}'},
        depends_on_past=False)

    # BranchPythonOperator
    # The next task depends on the return from the
    # python function check_api
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

    step2_EMR = DummyOperator(task_id='step2_EMR')

    step1_fargate >> check_running >> still_running
    step1_fargate >> check_running >> nothing_running >> step2_EMR
