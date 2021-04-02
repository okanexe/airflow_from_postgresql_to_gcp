import airflow
from airflow import DAG
from datetime import timedelta,datetime
from airflow.models import BaseOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage, bigquery
import os
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    '/Users/okans/Desktop/credit.json')

scoped_credentials = credentials.with_scopes(
    ['https://www.googleapis.com/auth/cloud-platform'])



os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/okans/Desktop/credit.json"
GOOGLE_APPLICATION_CREDENTIALS = '/Users/okans/Desktop/credit.json'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'storage_to_bq',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20))

storage_client = storage.Client.from_service_account_json('/Users/okans/Desktop/credit.json')

load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery_example',
    bucket='okans_database',
    source_objects=['2020-01-01.csv'],
    destination_project_dataset_table='amiable-vent-305512.dataset.datastorage',
    schema_fields = [
        {'name':'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name':'kullaniciKod', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name':'faturaAdresi', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name':'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name':'toplam', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name':'faturaKod', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name':'subeKodu', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    source_format= 'CSV',
    autodetect=False,
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)
