import psycopg2
import pandas as pd

import airflow
from airflow import DAG
from datetime import timedelta,datetime
from airflow.models import BaseOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage, bigquery
import os
from google.oauth2 import service_account
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

# start_date substract and end_date to obtain range for fetch data from database day by day
def date_range(start_date, end_date):
    rang = (datetime.strptime(end_date, '%Y-%m-%d').date() - datetime.strptime(start_date, '%Y-%m-%d').date()).days
    return rang

# table name from database
tablename = 'ordertable'
TABLES = []
# last date of updated table
#df = pd.read_csv('/Users/okans/Desktop/dates.csv')

#end_date = df[tablename]
#start_date = str(datetime.today().date())
start_date = '2020-01-01'
end_date = '2020-01-03'

def db_to_local(tablename, start_date, end_date):

    conn = psycopg2.connect("dbname=postgres user=okans password=")
    cur = conn.cursor()

    try:
        # between'de altsınırı alır üst sınırı almaz
        if (date_range(start_date, end_date) >= 1):
            date = datetime.strptime(start_date, '%Y-%m-%d').date()
            rang = date_range(start_date, end_date)
            for i in range(1, rang + 1):
                current_date = str(date)
                query = " select * from {} where created_at between \'{}\' and \'{}\' ".format(tablename, str(date),
                                                                                               str(date + timedelta(1)))
                date = date + timedelta(1)
                TABLES.append(str(current_date))
                print(date)
                # query = "select * from ordertable where created_at < \'{}\' ".format("2020-01-05")
                SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
                # Set up a variable to store our file path and name.
                t_path_n_file = "/Users/okans/Desktop/{}.csv".format(current_date)
                with open(t_path_n_file, 'w') as f_output:
                    cur.copy_expert(SQL_for_file_output, f_output)
        else:
            print("...enter correct parameter...")

    except:
        print("FAILED !!!!!")
        conn.rollback()
    else:
        # en son atılan datayı tutmak için current_date csv'ye atılacak.
        #df[tablename] = current_date
        #df.to_csv('/Users/okans/Desktop/dates.csv')
        conn.commit()

    conn.close()
    cur.close()



def local_to_storage(filename):
    bucket_name = "okans_database"
    source_file_name = "/Users/okans/Desktop/{}.csv".format(filename)
    destination_blob_name = "{}.csv".format(filename)

    storage_client = storage.Client.from_service_account_json('/Users/okans/Desktop/credit.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


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
    'airflow_pipeline',
    default_args=default_args,
    description='liveness monitoring dag',
    #start_date= datetime(year=2020, month=1, day=1), # from 2020-01-01 everyday at 23:55 fetch data from db to bigquery
    #schedule_interval='55 23 * * *',
    dagrun_timeout=timedelta(minutes=20))

storage_client = storage.Client.from_service_account_json('/Users/okans/Desktop/credit.json')

start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)
completed_task = DummyOperator(task_id="completed", dag=dag)

task1 = PythonOperator(
    task_id='db_to_local',
    provide_context=True,
    python_callable=db_to_local,
    op_kwargs={"tablename":tablename, "start_date" : start_date, "end_date" : end_date },
    dag=dag,
)

def localToGCP(tablename):
    task2 = PythonOperator(
        task_id='{}-task_to_gcp'.format(tablename),
        provide_context=True,
        python_callable=local_to_storage,
        op_kwargs={"filename": tablename},
        dag=dag,
    )
    return task2

def gcpToBq(table):
    task3 = GCSToBigQueryOperator(
        task_id='{}-task_to_bq'.format(table),
        bucket='okans_database',
        source_objects=['{}.csv'.format(table)],
        destination_project_dataset_table='amiable-vent-305512.dataset.{}'.format(table),
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
    return task3

# in here fetch data to desktop from database day by day. (tablename + date.csv) formatting
start_task >> task1 >> completed_task

TABLES = ['2020-01-01','2020-01-02']
# order's table for import to day by day. because of tables in storage as day by day .csv file.
for table in TABLES:
    export_table = localToGCP(table)
    import_table = gcpToBq(table)
    completed_task >> export_table >> import_table >> end_task
