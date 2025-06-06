from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from include.scraper import scrape_and_upload
from include.gcs_extract_upload import extract_upload_csvs_to_gcs_and_bigquery
from include.transform_and_upload_bigquery import transform_and_upload_to_bigquery
from include.pulling_bgquery_transform import combine_surveys
from include.final_transformation import clean_and_transform_survey_data


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

    # # Task 2: Extract CSVs from zip files and upload the CSV to GCS
    t2 = PythonOperator(
        task_id="gcs_extract_load",
        python_callable=extract_upload_csvs_to_gcs_and_bigquery,
    )

    t3 = PythonOperator(
        task_id="transform_and_upload_to_bigquery",
        python_callable=transform_and_upload_to_bigquery,
    )

    # t4 = PythonOperator(
    #     task_id="combine_surveys",
    #     python_callable=combine_surveys,
    # )

    # t5 = PythonOperator(
    #     task_id="clean_and_transform_survey_data",
    #     python_callable=clean_and_transform_survey_data,
    # )

    # Define dependencies
    t1 >> t2 >> t3
