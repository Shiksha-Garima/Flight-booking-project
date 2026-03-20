from datetime import datetime,timedelta
import uuid # Import UUID for unique batch ids
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable

# DAG default parameters
default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'start_date':datetime(2026,3,20)
}

# Define the DAG
with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule_interval=None, #Trigger manually or on-demand
    catchup=False,
) as dag:
    
    # Fetch environment variables
    env=Variable.get("env",default_var="dev")
    gcs_bucket=Variable.get("gcs_bucket", default_var="flight-booking-analysis")
    bq_project=Variable.get("bq_project",default_var="project-3d72aebf-2d6f-4b6c-884")
    bq_dataset=Variable.get("bq_dataset",default_var=f"flight_data_{env}")
    tables=Variable.get("tables",deserialize=True)

    # Extract table names from the 'tables' variable
    transformed_table=tables["transformed_table"]
    route_insights_table=tables["route_insights_table"]
    origin_insights_table=tables["origin_insights_table"]

    # Generate a unique batch id using uuid
    job_batch_id=f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"

    # Task 1 : File sensor for GCS
    file_sensor=GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket=gcs_bucket,
        object=f"flight-booking-analysis/source-{env}/flight_booking.csv", # Full file path in GCS
        google_cloud_conn_id="google_cloud_default",  # GCP Connection
        timeout=300, # Timeout in seconds
        poke_interval=30, # Time between checks
        mode="poke", # Blocking mode it will not free resource

    )

    # Task 2: Submit Pyspark job to Dataproc Serverless
    batch_details={
        "pyspark_batch":{
            "main_python_file_uri":f"gs://{gcs_bucket}/spark-job/spark_transformation_job.py", # Main python file
            "python_file_uris":[], # location of Python WHL Files if using
            "jar_file_uris":[], # location of JAR Files if required
            "args":[
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ]
        },
        "runtime_config":{
            "version":"2.2", # Specify Dataproc version (if needed)
        },
        "environment_config":{
            "execution_config":{
                "service_account":"841682174554-compute@developer.gserviceaccount.com",
                "network_uri":f"projects/{bq_project}/global/networks/default",
                "subnetwork_uri":f"projects/{bq_project}/regions/us-central1/subnetworks/default",
            }
        },
    }

    pyspark_task=DataprocCreateBatchOperator(
        task_id="run_spark_job_on_dataproc_serverless",
        batch=batch_details,
        batch_id=job_batch_id,
        project_id=bq_project,
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    # Task Dependencies
    file_sensor >> pyspark_task

