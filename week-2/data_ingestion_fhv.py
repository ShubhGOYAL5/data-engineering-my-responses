import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

from ingest_data_zones import format_to_parquet, upload_to_gcs
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# dataset_file = "zones.csv"
# parquet_file = dataset_file.replace('.csv', '.parquet')

URL_PREFIX = 'https://nyc-tlc.s3.amazonaws.com/trip+data'
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'

dataset_file = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
dataset_file_pq = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

AIRFLOW_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BUCKET = os.getenv('GCP_GCS_BUCKET')

local_airflow = DAG(
    "LocalIngestionFHVCatchUpMod-new",
    schedule_interval = "0 6 2 * *",
    start_date = datetime(2018, 12, 31),
    # end_date = datetime(2019, 12, 31)
    catchup = True,
    # max_active_runs = 3
)

with local_airflow:
    wget_task = BashOperator(
        task_id = "id-wget",
        bash_command = f'curl -sSLf {URL_TEMPLATE} > {AIRFLOW_PATH}/{dataset_file}'
    )

    list_dir_task = BashOperator(
        task_id = "id-listdir",
        bash_command = f'ls -lrt {AIRFLOW_PATH}'
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f'{AIRFLOW_PATH}/{dataset_file}',
        },
    )

    # local_to_gcs_task = PythonOperator(
    #     task_id = "id-uploadtogcsbucket",
    #     python_callable = upload_to_gcsbucket,
    #     op_kwargs = dict(
    #         bucket = BUCKET,
    #         target_path = 'raw/zones.parquet',
    #         source_path = f'{AIRFLOW_PATH}/{dataset_file}'
    #     )
    # )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{ dataset_file_pq }",
            "local_file": f"{ AIRFLOW_PATH }/{ dataset_file_pq }",
        },
    )

    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "zones",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/zones.parquet"],
    #             # "sourceUris": ['gs://dtc_data_lake_dtc-de-338618/raw/zones.parquet']
    #         },
    #     },
    # )

    # ingest_task = PythonOperator(
    #     task_id = "id-ingest",
    #     python_callable = ingest_data,
    #     op_kwargs = dict(
    #         host = PG_HOST,
    #         user = PG_USER,
    #         password = PG_PASSWORD,
    #         port = PG_PORT,
    #         db = PG_DATABASE,
    #         table_name = 'sometable',
    #         csv_file = 'taxi+_zone_lookup.csv'
    #     )
    # )

    # wget_task >> format_to_parquet_task >> list_dir_task >> local_to_gcs_task >> bigquery_external_table_task
    wget_task >> list_dir_task >> format_to_parquet_task >> local_to_gcs_task
    #  >> local_to_gcs_task