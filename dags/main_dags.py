import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

from include.scraper import scrape_and_upload
from include.gcs_extract_upload import extract_upload_csvs_to_gcs_and_bigquery
from include.transform_and_upload_bigquery import transform_and_upload_to_bigquery

# Path to the service account file
SERVICE_ACCOUNT_PATH = "/usr/local/airflow/gcloud/application_default_credentials.json"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="bigquery_conn",
        profile_args={
            "project": "stackoverflow-survey-456106",
            "dataset": "stackoverflow_survey_dataset",
            "location": "US",
            "keyfile": SERVICE_ACCOUNT_PATH,
        },
    ),
)

with DAG(
    dag_id="stackoverflow_end_to_end_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["python", "scraper", "spark", "dbt", "stackoverflow"],
) as dag:

    # Task 1: Scrape and upload to GCS
    t1 = PythonOperator(
        task_id="scrape_and_upload_to_gcs",
        python_callable=scrape_and_upload,
    )

    # Task 2: Extract CSVs from zip files and upload the CSV to GCS
    t2 = PythonOperator(
        task_id="gcs_extract_load",
        python_callable=extract_upload_csvs_to_gcs_and_bigquery,
    )

    # Task 3: Transform and upload cleaned data to BigQuery
    t3 = PythonOperator(
        task_id="transform_and_upload_to_bigquery",
        python_callable=transform_and_upload_to_bigquery,
    )

    # Task 4: dbt transformations
    dbt_taskgroup = DbtTaskGroup(
        group_id="dbt_transformation",
        project_config=ProjectConfig("/usr/local/airflow/dbt_pipeline"),
        operator_args={"install_deps": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
        ),
    )

    # Set dependencies
    t1 >> t2 >> t3 >> dbt_taskgroup
