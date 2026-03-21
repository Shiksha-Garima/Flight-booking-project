from datetime import datetime,timedelta
import uuid # Import UUID for unique batch ids
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago

# DAG default parameters
default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'retries':2,
    'retry_delay':timedelta(minutes=2),
    'start_date':datetime(2026,3,21),
}

# Define the DAG
with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule_interval='@daily', #Trigger manually or on-demand
    catchup=False,
) as dag:
    
    # Fetch environment variables
    env=Variable.get("env",default_var="dev")
    gcs_bucket=Variable.get("gcs_bucket", default_var="flight-booking-analysis")
    bq_project=Variable.get("bq_project",default_var="project-3d72aebf-2d6f-4b6c-884")
    bq_dataset=Variable.get("bq_dataset",default_var=f"flight_data_{env}")
    tables=Variable.get("tables",deserialize_json=True)

    # Extract table names from the 'tables' variable
    transformed_table=tables["transformed_table"]
    route_insights_table=tables["route_insights_table"]
    origin_insights_table=tables["origin_insights_table"]

    # Define cluster config
    CLUSTER_NAME='dataproc-spark-airflow-dev'
    PROJECT_ID='project-3d72aebf-2d6f-4b6c-884'
    REGION='us-east1'
    
    CLUSTER_CONFIG={
    'gce_cluster_config': {
        'zone_uri': 'us-east1-c'
    },
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'e2-standard-4',
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'worker_config': {
        'num_instances': 0
    },
    'software_config': {
        'image_version': '2.2.26-debian12'
    }
}
    # Task 1

    file_sensor = GCSObjectExistenceSensor(
    task_id="check_file_arrival",
    bucket=gcs_bucket,
    object=f"source-{env}/flight_booking.csv",  # correct path
    google_cloud_conn_id="google_cloud_default",
    timeout=600,
    poke_interval=30,
    mode="reschedule",
)
    # Task 2
    create_cluster=DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    gcp_conn_id="google_cloud_default",
    )
    
    pyspark_job = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{gcs_bucket}/spark-job/spark_transformation_job.py",
        "args": [
            f"--env={env}",
            f"--bq_project={bq_project}",
            f"--bq_dataset={bq_dataset}",
            f"--transformed_table={transformed_table}",
            f"--route_insights_table={route_insights_table}",
            f"--origin_insights_table={origin_insights_table}",
        ],
    },
}
    
    # Task 3
    submit_pyspark_job = DataprocSubmitJobOperator(
    task_id="submit_pyspark_job_on_dataproc",
    job=pyspark_job,
    region="us-east1",
    project_id=PROJECT_ID,
    gcp_conn_id="google_cloud_default",
    )

    # Task 4
    delete_cluster=DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done', # ensures cluster deletion even if spark job fails
    )   


    file_sensor >> create_cluster >> submit_pyspark_job >> delete_cluster