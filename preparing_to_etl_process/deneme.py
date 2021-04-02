from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
from datetime import datetime, timedelta
from airflow import DAG


default_args = {
    'owner': 'okan',
    'start_date': datetime.now()-timedelta(minutes=1)
    }

temp_command="""{% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}"""



# Defining the DAG using Context Manager
with  DAG(
        'deneme',
        default_args=default_args,
        schedule_interval=None,
        ) as dag:
        t1 = BashOperator(
                task_id = 'task1',
                bash_command = temp_command,

        )
        t2 = BashOperator(
                task_id = 'task2',
                bash_command = 'task2.sh',
        )
        t1 >> t2 # Defining the task dependencies
