"""
DAG use to create Data Warehouse in Google BigQuery to accomplish final project
from DigitalSkola class.
"""

from datetime import datetime
from datetime import timedelta

from sql.dwh import *

from airflow import DAG
from airflow.models import Variable
from airflow.utils import trigger_rule
from airflow.utils.task_group import TaskGroup

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitPySparkJobOperator
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSListObjectsOperator,
    GCSDeleteObjectsOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

# Global configuration
DAG_ID = 'data-warehouse-dag'

config = Variable.get(DAG_ID, deserialize_json=True)
cluster = config['dataproc']
bigquery = config['bq']
gcs = config['gcs']

PROJECT_ID = config['project_id']

# Cluster setting for Dataproc
REGION = cluster['region']
CLUSTER_NAME = cluster['cluster_name']
ZONE = cluster['zone']
NUM_WORKERS = int(cluster['num_workers'])
MASTER = cluster['master_machine']
WORKER = cluster['worker_machine']
MASTER_SIZE = int(cluster['master_disk_size'])
WORKER_SIZE = int(cluster['worker_disk_size'])

# Connection ID
BQ_CONN_ID = bigquery['bq_conn_id']
GCS_CONN_ID = gcs['gcs_conn_id']

# Bucket name
BUCKET = gcs['main_bucket']
RAW = gcs['source_bucket']

# Dataset name
STAGING = bigquery['staging_dataset']
DWH = bigquery['dwh_dataset']

# External file
SPARK_CONVERT_PARQUET_URI = f'gs://{BUCKET}/src/convert_to_parquet.py'

# List tablename
FILES = gcs['files']

filenames = [
                ("uscountry", "UScountry"),
                ("immigration_data_sample", "immigration_data_sample"),
                ("usport","USPORT"),
                ("usstate", "USSTATE"),
                ("globallandtemperaturesbycity", "GlobalLandTemperatureByCity"),
                ("us_cities_demographics","us-cities-demographics"),
                ("airport_codes_csv","airport-codes_csv"),
                ("orders","orders")
            ]

# List query
query_list = [
    ('immigration', DWH_IMMIGRATION),
    ('weather', DWH_WEATHER),
    ('demo', DWH_DEMO),
    ('country', DWH_COUNTRY),
    ('port', DWH_PORT),
    ('state', DWH_STATE),
    ('time', DWH_TIME)
]


# Default arguments
default_args = {
    'start_date' : datetime(2022,2,26),
    'end_date' : None,
    'depends_on_past' : False,
    'email_on_failure' : False,
    'email_on_success' : False,
    'retries' : 2,
    'retry_delay' : timedelta(minutes=5)
}


with DAG(
    dag_id = DAG_ID,
    description='''
    Convert CSV files to PARQUET format, transfer it into staging and 
    create data warehouse in Google BigQuery''',
    schedule_interval = None,
    default_args=default_args,
    catchup=False,
    tags=['final-project','digitalskola','testing']
) as dag:

    start_task = BashOperator(
        task_id='start_process',
        bash_command='echo "Start at $(date)"'
    )

    end_task = BashOperator(
        task_id='end_process',
        bash_command='echo "Finish at $(date)"'
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        num_workers=NUM_WORKERS,
        zone=ZONE,
        region=REGION,
        master_machine_type=MASTER,
        master_disk_size=MASTER_SIZE,
        worker_machine_type=WORKER,
        worker_disk_size=WORKER_SIZE        
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )

    finish_transfer = DummyOperator(
        task_id='reporting_finish_transfer'
    )

    start_dwh = DummyOperator(
        task_id='starting_data_warehouse_process'
    )

    # delete_parquet = GCSDeleteObjectsOperator(
    #     task_id='delete_parquet_files',
    #     bucket_name=f'{RAW}',
    #     prefix='staging/',
    #     gcp_conn_id=GCS_CONN_ID,
    # )

    create_parquet = DataprocSubmitPySparkJobOperator(
        task_id='create_parquet_file',
        cluster_name=CLUSTER_NAME,
        main=SPARK_CONVERT_PARQUET_URI,
        project_id=PROJECT_ID,
        region=REGION,
    )

    transfer_staging = [GCSToBigQueryOperator(
        task_id=f'move_{new}_to_bigquery',
        bucket=f'{RAW}',
        source_objects=[f'staging/{new}.parquet/part*'],
        source_format='PARQUET',
        destination_project_dataset_table=f'{PROJECT_ID}:{STAGING}.{new}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id=BQ_CONN_ID,
        google_cloud_storage_conn_id=GCS_CONN_ID,
        autodetect=True   
    ) for new, _ in filenames]
   
    create_dwh = [BigQueryInsertJobOperator(
        task_id=f'create_dwh_for_table_{name}',
        gcp_conn_id=GCS_CONN_ID,
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False
            }
        }
    ) for name, query in query_list]

    # slack_op = SlackAPIPostOperator(
    #     task_id='test-slack',
    #     channel='#random',
    #     username='Airflow for Slack',
    #     text="Hurray it's works!"
    # )

    # Set dependencies
    start_task >> create_cluster >> create_parquet >> delete_cluster >> end_task
    create_parquet >> transfer_staging >> finish_transfer
    finish_transfer >> start_dwh >> create_dwh >> end_task

