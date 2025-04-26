import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# Path to the service account file mounted in the container
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
            "keyfile": SERVICE_ACCOUNT_PATH,  # Use the mounted service account file
        },
    ),
)

dbt_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig("/usr/local/airflow/dbt_pipeline",),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_dag",
    default_args={"retries": 2},
)
