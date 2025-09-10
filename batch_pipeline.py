from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator, DataprocDeleteBatchOperator
import requests
from airflow.utils.trigger_rule import TriggerRule
import json
# puplish notifcation with name of file added and path to it

PROJECT_ID = "real-time-cloud-data-pipeline"
TOPIC_ID = "file_name_arrived"
YEAR = Variable.get("year")
MONTH = Variable.get("month") 

# requriments for batch job
REGION = "us-central1"
PY_FILE_URI = "gs://dataproc_jobs_stream_batch/batch_script/dat_proc_batch_file.py" #Where file of batch is found
JAR_URIS = [
    "gs://dataproc_jobs_stream_batch/needed_jars/snowflake-jdbc-3.24.2.jar",
    "gs://dataproc_jobs_stream_batch/needed_jars/spark-snowflake_2.13-3.1.4.jar",
] # jars used to connect and write to snowflake
DEFAULT_INPUT__ARG_1 = f"gs://batch_data_gcs/yellow_tripdata_{YEAR}-{MONTH}.parquet" # the argument that has file name

bucket = "batch_data_gcs"
object_name = "row_count/row_count.json"  # relative path inside bucket
hook = GCSHook()

# Pick the first JSON file under the prefix
content = hook.download(bucket, object_name).decode("utf-8")
data = json.loads(content)
row_count = data["row_count"]

DEFAULT_INPUT__ARG_2 = row_count
batch_config = {
        "pyspark_batch": {
            "main_python_file_uri": PY_FILE_URI,
            # Arguments after `--` in gcloud go here:
            "args": [
                "{{ dag_run.conf.get('input_file', '" + DEFAULT_INPUT__ARG_1 + "') }}",
                "{{ dag_run.conf.get('row_number_count', '" + str(DEFAULT_INPUT__ARG_2) + "') }}"
            ],
            "jar_file_uris": JAR_URIS,
        },
        "environment_config": {
            "execution_config": {
                # subnet you used in gcloud to allow outbound for serverless spark:
                "subnetwork_uri": "projects/real-time-cloud-data-pipeline/regions/us-central1/subnetworks/default",
            }
        },
        # Runtime version is your `--version`
        "runtime_config": {"version": "2.3"},
    }

# to stop the dag from running if we hit year 2025
def year_check():
    if int(YEAR) == 2025:
        raise AirflowSkipException("Stopping DAG: condition not met")

# to incrment the month value and get other folders
def increment_month_year(): 
    if int(MONTH) < 9:
        Variable.set("month", "0"+str(int(MONTH)+1))
    else:
        Variable.set("month", str(int(MONTH)+1))
    
    if int(MONTH) % 12 == 0:
        Variable.set("year", str(int(YEAR)+1))
        Variable.set("month", "01")


# to get needed files in the bucket
def url_to_gcs():
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YEAR}-{MONTH}.parquet" 
    r = requests.get(url, timeout=20)
    GCSHook(gcp_conn_id="google_cloud_default").upload(
        bucket_name="batch_data_gcs"  ,
        object_name=f"yellow_tripdata_{YEAR}-{MONTH}.parquet" ,
        data=r.content,            # upload bytes directly (no disk)
        mime_type="application/octet-stream",
    )

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1),
    #'retries': 1,
}


# Define the DAG
with DAG(
    dag_id='batch_pipline',
    default_args=default_args,
    description='A simple dag to upload data from nyc webiste to gcs bucket for batch processing',
    schedule_interval=timedelta(hours=3),  # runs every 
    catchup=False,  # to prevent the dag from trying to run agian and catch days it didnt run
) as dag:
    
    
    year_limit_check = PythonOperator(
        task_id = "check_which_year_to_stop_at",
        python_callable=year_check
    )

    upload = PythonOperator(
        task_id = "add_file_to_bucket",
        python_callable=url_to_gcs
    )


    create_batch = DataprocCreateBatchOperator(
        task_id="create_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch=batch_config,
        # set to True in newer provider versions to wait server-side; otherwise use the sensor below
        deferrable=False,
    )

    """ delete_batch = DataprocDeleteBatchOperator(
        task_id="delete_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID,
        trigger_rule=TriggerRule.ALL_SUCCESS,   # ensure this runs even if the job failed
    ) """
    
    add_next_month_year = PythonOperator(
        task_id = "increment_month_check_year",
        python_callable=increment_month_year
    )
    
    
    
    year_limit_check >> upload >> create_batch >>add_next_month_year 

    