from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.scraper import scrape_and_upload
from include.gcs_extract_upload import extract_upload_csvs_to_gcs_and_bigquery


with DAG(
    dag_id="stackoverflow_scraper_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["scraper", "stackoverflow"],
) as dag:

    # Task 1: Scrape and upload to GCS
    t1 = PythonOperator(
        task_id="scrape_and_upload_to_gcs",
        python_callable=scrape_and_upload,
    )

    t2 = PythonOperator(
        task_id="gcs_extract_load",
        python_callable=extract_upload_csvs_to_gcs_and_bigquery,
    )

    # Define dependencies
    t1 >> t2
