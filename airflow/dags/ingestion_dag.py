import os
import logging
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime
import zipfile


PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_GCS_BUCKET")
DATASET_NAME = os.getenv("BIGQUERY_DATASET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = "https://divvy-tripdata.s3.amazonaws.com"
URL_TEMPLATE = URL_PREFIX + "/{{logical_date.strftime('%Y%m')}}-divvy-tripdata.zip"
file_name = "{{logical_date.strftime('%Y%m')}}-divvy-tripdata.zip"
OUTPUT_FILE_TEMPLATE = f"{AIRFLOW_HOME}/{file_name}"
parquet_file = file_name.replace('.zip', '.parquet')
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 5,
    'start_date':datetime(2025, 4, 1),
}

def format_to_parquet(src_file):

    if not src_file.endswith('.zip'):

        logging.error("Can only accept source files in ZIP format, for the moment")

        return

    print(f"Extracting .CSV file")

    with zipfile.ZipFile(src_file) as z:

        csv_file = [f for f in z.namelist() if f.endswith('.csv') and not f.startswith('__MACOSX/')]

        if not csv_file:

            logging.error("No CSV file found in the ZIP archive")

            return
        else:

            table = pv.read_csv(z.open(csv_file[0])) #Read the CSV file directly from the ZIP archive into a PyArrow table

    #convert to parquet and save locally
    pq.write_table(table, src_file.replace('.zip', '.parquet'))

def upload_to_gcs(bucket_name, object_name, local_file):

    #WORKAROUND to prevent timeout for files > 5 MB on 800 kbps upload speed.
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


with DAG(
    dag_id="GCPIngestionDag",
    schedule="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1
) as dag:

    download_data = BashOperator(
        task_id="download_data",
        bash_command=f"echo 'Downloading {file_name}...' && curl -sSLf {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet",
        python_callable=format_to_parquet,
        op_kwargs=dict(src_file=OUTPUT_FILE_TEMPLATE)
    )
    upload_bucket = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs=dict(
            bucket_name=BUCKET_NAME,
            object_name=f"raw/{parquet_file}",
            local_file=OUTPUT_FILE_TEMPLATE.replace('.zip', '.parquet')
        )
    )
    load_data = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[f"raw/{parquet_file}"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.trips",
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE'
    )
    cleanup_task = BashOperator(
        task_id="cleanup",
        bash_command=f"echo 'Cleaning up {file_name}...' && rm -f {OUTPUT_FILE_TEMPLATE} {OUTPUT_FILE_TEMPLATE.replace('.zip', '.parquet')}"
    )
    dbt_transformation = BashOperator(
    task_id="dbt_transformation",
    bash_command=(
        "cd /opt/airflow/dbt/bikeshare_pipeline && "
        "dbt deps --profiles-dir /opt/airflow/.dbt && "
        "dbt run --profiles-dir /opt/airflow/.dbt --target prod && "
        "dbt test --profiles-dir /opt/airflow/.dbt --target prod && "
        "dbt docs generate --profiles-dir /opt/airflow/.dbt --target prod"
    )
)

    download_data >> format_to_parquet_task >> upload_bucket >> load_data >> cleanup_task >> dbt_transformation