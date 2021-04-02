import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta,datetime


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'migrate',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20))

t1 = BashOperator(
    task_id='task1',
    bash_command="echo hi1",
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31-1)
t2 = BashOperator(
    task_id='task2',
    bash_command="echo hi2",
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31-1)
t1 >> t2
