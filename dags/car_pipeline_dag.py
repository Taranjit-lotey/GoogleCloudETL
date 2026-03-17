from airflow import DAG
from airflow.providers.google.cloud.operator.dataproc import \
      (DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataprocSubmitJobOperator
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

CLUSTER_NAME = "car-listings-cluster"
PROJECT_ID = "etl-migration-1"
REGION = "us-central1"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2", #
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 32
        }
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 32
        }
    },
    "software_config": {
        "image_version": "2.0-debian10"
    }
}