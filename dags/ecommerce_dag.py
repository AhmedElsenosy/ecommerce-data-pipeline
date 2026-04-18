"""
E-Commerce Data Pipeline - Airflow DAG
=======================================
This DAG automates the full ETL pipeline:
  Task 1: Create HDFS directories
  Task 2: Upload CSV files to HDFS
  Task 3: Run Spark ETL job
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# ============================================
# DAG DEFAULT ARGUMENTS
# ============================================
# These settings apply to all tasks in the DAG

default_args = {
    'owner': 'airflow',               # Who owns this DAG
    'depends_on_past': False,          # Don't wait for previous runs
    'email_on_failure': False,         # Don't send emails on failure
    'email_on_retry': False,           # Don't send emails on retry
    'retries': 2,                      # Retry failed tasks 2 times
    'retry_delay': timedelta(minutes=1),  # Wait 1 min between retries
}


# ============================================
# DEFINE THE DAG
# ============================================
# schedule=None means it won't run automatically,
# you trigger it manually from the Airflow UI

dag = DAG(
    dag_id='ecommerce_etl_pipeline',       # Name shown in Airflow UI
    default_args=default_args,
    description='E-Commerce ETL: CSV -> HDFS -> Spark -> Parquet',
    schedule=None,                          # Manual trigger only
    start_date=datetime(2026, 1, 1),        # Required but not used with schedule=None
    catchup=False,                          # Don't run for past dates
    tags=['ecommerce', 'etl', 'spark', 'hdfs'],  # Tags for easy search in UI
)


# ============================================
# TASK 1: Create HDFS Directories
# ============================================
# Uses the hdfs Python library (WebHDFS REST API)
# to create /data/raw and /data/processed in HDFS

def create_hdfs_directories():
    """Create the necessary HDFS directories for raw and processed data."""
    from hdfs import InsecureClient

    # Connect to HDFS NameNode via WebHDFS API (port 9870)
    client = InsecureClient('http://namenode:9870', user='root')

    # Create directories (makedirs works like mkdir -p)
    directories = ['/data/raw', '/data/processed']

    for directory in directories:
        client.makedirs(directory)
        # Set permissions so Spark (user=spark) can write
        client.set_permission(directory, '777')
        print(f"Created directory: {directory}")

    print("\nAll HDFS directories ready!")


task_create_dirs = PythonOperator(
    task_id='create_hdfs_directories',       # Task name in Airflow UI
    python_callable=create_hdfs_directories,  # Function to run
    dag=dag,
)


# ============================================
# TASK 2: Upload CSV Files to HDFS
# ============================================
# Reads CSV files from local /opt/airflow/data/
# and uploads them to HDFS /data/raw/

def upload_csvs_to_hdfs():
    """Upload all CSV files from local storage to HDFS."""
    from hdfs import InsecureClient
    import os

    client = InsecureClient('http://namenode:9870', user='root')

    # Local path where CSVs are mounted (from docker-compose volumes)
    local_data_dir = '/opt/airflow/data'

    # List of CSV files to upload
    csv_files = [
        'users.csv',
        'products.csv',
        'orders.csv',
        'order_items.csv',
        'reviews.csv',
        'events.csv',
    ]

    for csv_file in csv_files:
        local_path = os.path.join(local_data_dir, csv_file)
        hdfs_path = f'/data/raw/{csv_file}'

        # Check if file exists locally
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"CSV file not found: {local_path}")

        # Upload to HDFS (overwrite if exists)
        client.upload(hdfs_path, local_path, overwrite=True)
        file_size = os.path.getsize(local_path)
        print(f"Uploaded {csv_file} ({file_size:,} bytes) -> {hdfs_path}")

    # Verify all files are in HDFS
    hdfs_files = client.list('/data/raw')
    print(f"\nFiles in HDFS /data/raw/: {hdfs_files}")
    print(f"Total files uploaded: {len(hdfs_files)}")


task_upload_csvs = PythonOperator(
    task_id='upload_csvs_to_hdfs',
    python_callable=upload_csvs_to_hdfs,
    dag=dag,
)


# ============================================
# TASK 3: Run Spark ETL Job
# ============================================
# Submits the etl_job.py to the Spark cluster
# using spark-submit from the Airflow container

task_run_spark_etl = BashOperator(
    task_id='run_spark_etl',
    bash_command=(
        '$SPARK_HOME/bin/spark-submit '
        '--master spark://spark-master:7077 '
        '--deploy-mode client '
        '--driver-memory 1g '
        '--executor-memory 1g '
        '/opt/airflow/spark-apps/etl_job.py'
    ),
    dag=dag,
)


# ============================================
# TASK DEPENDENCIES (Execution Order)
# ============================================
# This defines the order: Task 1 -> Task 2 -> Task 3
# Task 2 won't start until Task 1 finishes
# Task 3 won't start until Task 2 finishes

task_create_dirs >> task_upload_csvs >> task_run_spark_etl
