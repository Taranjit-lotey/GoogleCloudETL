from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)

#days_ago is used to prevent backfilling a dynamic way
from airflow.utils.dates import days_ago
#timedelta used for retry delay
from datetime import timedelta, datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries":2,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG( 
    dag_id = "car_listings_pipeline",
    default_args = default_args,
    description = "A DAG to process car listings data using Dataproc",
    schedule_interval = "0 6 * * *",  # Daily at 6 AM
    start_date = days_ago(1),  # Start from yesterday to avoid backfilling
    catchup = False,  # Don't backfill missed runs
    tags =["car_listings", "etl"]
)

#create a existing sensor checking raw file landing

create_sensor = GCSObjectExistenceSensor(
    task_id="check_raw_file",
    bucket="etl-migration-1-data",
    object="raw/messy_cars.csv",
    google_cloud_conn_id="google_cloud_default",
    timeout=600,  # Timeout after 10 minutes
    poke_interval=30,  # Check every 30 seconds
    dag=dag
)

CLUSTER_NAME = "car-listings-cluster"
PROJECT_ID = "etl-migration-1"
REGION = "us-central1"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-4",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 32
        }
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "e2-standard-4",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 32
        }
    },
    "software_config": {
        "image_version": "2.0-debian10"
    },
    "gce_cluster_config": {
        "zone_uri": ""
    }
}

create_cluster = DataprocCreateClusterOperator(
    task_id = "create_cluster",
    project_id = PROJECT_ID,
    cluster_config = CLUSTER_CONFIG,
    region = REGION,
    cluster_name = CLUSTER_NAME,
    dag = dag
)

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://etl-migration-1-data/scripts/transform.py",
        "args": [
            "--input_path",          "gs://etl-migration-1-data/raw/messy_cars.csv",
            "--output_path",         "gs://etl-migration-1-data/processed/",
            "--watermark_date", "2020-01-01"
        ],
        "jar_file_uris": [
            "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
        ]
    }
}

submit_job = DataprocSubmitJobOperator(
    task_id = "submit_pyspark_job",
    job = PYSPARK_JOB,
    region = REGION,
    project_id = PROJECT_ID,
    retries=0,
    dag = dag
)

# Intentionally slow job to demonstrate Spark UI anti-patterns:
# data skew, shuffle spike, GC pressure, spill to disk
SLOW_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://etl-migration-1-data/scripts/slow_job.py",
        "args": [
            "--input_path",  "gs://etl-migration-1-data/raw/messy_cars.csv",
            "--output_path", "gs://etl-migration-1-data/slow_job_output/"
        ]
    }
}

submit_slow_job = DataprocSubmitJobOperator(
    task_id="submit_slow_job",
    job=SLOW_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    retries=0,
    dag=dag
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    trigger_rule="all_done",
    dag=dag
)


create_cluster >> submit_job >> submit_slow_job >> delete_cluster

