"""
DAG use to create Data Warehouse in Google BigQuery to accomplish final project
from DigitalSkola class.
"""

from datetime import datetime
from datetime import timedelta

from sql.dwh import *
from sql.dwh import query_list

from airflow import DAG
from airflow.models import Variable
from airflow.utils import trigger_rule
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitPySparkJobOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
)


# Global configuration
DAG_ID = 'data-warehouse-dag'

config = Variable.get(DAG_ID, deserialize_json=True)
cluster = config['dataproc']
bigquery = config['bq']
gcs = config['gcs']

PROJECT_ID = config['project_id']
DAG_DESC = config['description']

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
SPARK_CONVERT_PARQUET_URI = gcs['pyspark_file']

# List filename
FILES = gcs['files']

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
    description=DAG_DESC,
    schedule_interval = None,  # triggered manually
    default_args=default_args,
    catchup=False,
    tags=['final-project','digitalskola']
) as dag:

    start_task = DummyOperator(
        task_id='start_process',
    )

    end_task = DummyOperator(
        task_id='end_process',
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

    create_parquet = DataprocSubmitPySparkJobOperator(
        task_id='create_parquet_file',
        cluster_name=CLUSTER_NAME,
        main=SPARK_CONVERT_PARQUET_URI,
        project_id=PROJECT_ID,
        region=REGION
    )

    finish_create_table = DummyOperator(
        task_id='finish_create_external_tables'
    )

    start_dwh = DummyOperator(
        task_id='start_data_warehousing'
    )

    create_external_table = [BigQueryCreateExternalTableOperator(
        task_id=f'create_external_table_{name}_in_bigquery',
        bucket=f'{RAW}',
        source_objects=[f'staging/{name}.parquet/part*'],
        source_format='PARQUET',
        destination_project_dataset_table=f'{PROJECT_ID}.{STAGING}.{name}',
        bigquery_conn_id=BQ_CONN_ID,
        google_cloud_storage_conn_id=GCS_CONN_ID  
    ) for name in FILES]
   
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

    # Set task dependencies
    start_task >> create_cluster >> create_parquet >> delete_cluster >> end_task
    create_parquet >> create_external_table >> finish_create_table
    finish_create_table >> start_dwh >> create_dwh >> end_task

