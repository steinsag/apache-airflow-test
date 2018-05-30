from datetime import datetime, timedelta
from pprint import pprint
from time import sleep

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.state import State

# introduce global IDs for each task
from airflow.utils.trigger_rule import TriggerRule

TASK_ID_EXTRACT_DATA = 'ExtractData'
TASK_ID_LOAD_DATA = 'LoadData'
TASK_ID_CHANGE_SYSTEM_1 = 'ChangeSystem1'
TASK_ID_CHANGE_SYSTEM_2 = 'ChangeSystem2'
TASK_ID_CHANGE_SYSTEM_3 = 'ChangeSystem3'
TASK_ID_CHECK_SUCCESS = 'CheckSuccess'
TASK_ID_ROLLBACK = 'Rollback'
TASK_ID_DONE = 'Done'

# introduce global IDs for Xcom keys
XCOM_KEY_INIT_ARGS = 'init_args'


class RollbackTriggeredException(Exception):
    """An exception to mark that a workflow was rolled back."""
    pass


def extract_input(**context):
    # access to input arguments via conf
    init_args = context['dag_run'].conf

    # pushing input arguments to Xcom with key init_args
    context['ti'].xcom_push(XCOM_KEY_INIT_ARGS, init_args)


def load_data(**context):
    # to check what is avaible in context
    pprint(context)

    # get input arguments from Xcom
    init_args = context['ti'].xcom_pull(task_ids=None, key=XCOM_KEY_INIT_ARGS)

    # some DB query to load details
    init_args['attributeFromDB1'] = 'some value loaded from DB'
    init_args['attributeFromDB2'] = 'another value loaded from DB'

    # by returning a variable, it gets automatically added to Xcom
    return init_args


def system_1_change(**context):
    # load data returned by specific task
    attrs = context['ti'].xcom_pull(task_ids=TASK_ID_LOAD_DATA)

    input_attribute_1 = attrs['inputAttribute1']
    input_attribute_2 = attrs['inputAttribute2']
    attribute_from_db_1 = attrs['attributeFromDB1']
    attribute_from_db_2 = attrs['attributeFromDB2']

    pprint('Input Attribute 1: ' + input_attribute_1)
    pprint('Input Attribute 2: ' + input_attribute_2)
    pprint('Attribute From DB 1: ' + attribute_from_db_1)
    pprint('Attribute From DB 2: ' + attribute_from_db_2)

    pprint('Changing on System 3 ' + input_attribute_1)

    sleep(3)


def system_2_change(**context):
    attrs = context['ti'].xcom_pull(task_ids=TASK_ID_LOAD_DATA)

    attribute_from_db_1 = attrs['inputAttribute1']

    pprint('Changing on System 2 ' + attribute_from_db_1)

    sleep(1)


def system_3_change(**context):
    ti = context['ti']
    attrs = ti.xcom_pull(task_ids=TASK_ID_LOAD_DATA)

    attribute_from_db_2 = attrs['attributeFromDB2']

    pprint('Changing on System 3 ' + attribute_from_db_2)

    sleep(2)

    # uncomment following line to mark this task as failed
    # raise ConnectionAbortedError('Raising an error while changing on System 3 ' + attribute_from_db_2)


def check_success(**context):
    """this task returns TASK_ID_ROLLBACK if any task failed, otherwise TASK_ID_DONE"""
    ti = context['ti']

    # get current workflow execution
    dag_run = ti.get_dagrun()

    # get any failed task
    tasks = dag_run.get_task_instances(state=State.FAILED)

    # return TASK_ID_ROLLBACK if there are any failed tasks
    if tasks:
        pprint('Tasks with errors:')
        pprint(tasks)

        for task in tasks:
            pprint(task.task_id)
        return TASK_ID_ROLLBACK
    else:
        pprint('No tasks with errors found.')
        return TASK_ID_DONE


def rollback():
    pprint('Executing rollback...')

    # bug in Airflow 1.9: Workflow only fails if last task has failed, so we need to raise this exception
    raise RollbackTriggeredException('This workflow was rolled back due to a failure.')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# create workflow
workflow = DAG('SAMPLE_WORKFLOW_TYPE', default_args=default_args, schedule_interval=None, catchup=False)

# add tasks to workflow
task_extract_input = PythonOperator(task_id=TASK_ID_EXTRACT_DATA, dag=workflow, provide_context=True,
                                    python_callable=extract_input)
task_load_data = PythonOperator(task_id=TASK_ID_LOAD_DATA, dag=workflow, provide_context=True, python_callable=load_data)

task_system_1 = PythonOperator(task_id=TASK_ID_CHANGE_SYSTEM_1, dag=workflow, provide_context=True,
                               python_callable=system_1_change)
task_system_2 = PythonOperator(task_id=TASK_ID_CHANGE_SYSTEM_2, dag=workflow, provide_context=True,
                               python_callable=system_2_change)
task_system_3 = PythonOperator(task_id=TASK_ID_CHANGE_SYSTEM_3, dag=workflow, provide_context=True,
                               python_callable=system_3_change)

task_check_success = BranchPythonOperator(task_id=TASK_ID_CHECK_SUCCESS, dag=workflow, provide_context=True,
                                          python_callable=check_success, trigger_rule=TriggerRule.ALL_DONE)
task_rollback = PythonOperator(task_id=TASK_ID_ROLLBACK, dag=workflow, provide_context=False, python_callable=rollback)
task_done = DummyOperator(task_id=TASK_ID_DONE, dag=workflow)

# define relationship between tasks (set up directed acyclic graph
task_extract_input >> task_load_data

task_load_data >> task_system_1 >> task_check_success
task_load_data >> task_system_2 >> task_check_success
task_load_data >> task_system_3 >> task_check_success

task_check_success >> task_rollback
task_check_success >> task_done
