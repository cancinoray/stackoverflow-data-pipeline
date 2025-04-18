from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, concat_ws, monotonically_increasing_id
from pyspark.sql import types
from google.cloud import storage
import os

from transforms.transform_2011 import transform_2011
from transforms.transform_2012 import transform_2012
from transforms.transform_2013 import transform_2013
from transforms.transform_2014 import transform_2014
from transforms.transform_2015 import transform_2015
from transforms.transform_2016 import transform_2016
from transforms.transform_2017 import transform_2017
from transforms.transform_2018 import transform_2018
from transforms.transform_2019 import transform_2019
from transforms.transform_2020 import transform_2020
from transforms.transform_2021 import transform_2021
from transforms.transform_2022 import transform_2022
from transforms.transform_2023 import transform_2023
from transforms.transform_2024 import transform_2024


def apply_transformation(df, year):
    transform_functions = {
        2011: transform_2011,
        2012: transform_2012,
        2013: transform_2013,
        2014: transform_2014,
        2015: transform_2015,
        2016: transform_2016,
        2017: transform_2017,
        2018: transform_2018,
        2019: transform_2019,
        2020: transform_2020,
        2021: transform_2021,
        2022: transform_2022,
        2023: transform_2023,
        2024: transform_2024,
    }

    transform_func = transform_functions.get(year)
    if transform_func:
        return transform_func(df)
    else:
        raise ValueError(f"No transformation defined for year {year}")


# ========== Main Orchestration Function ==========
def transform__and_upload_to_bigquery(start_year=2011, end_year=2024):
    bucket_name = os.getenv(
        "GCS_BUCKET", "gsbucket-stackoverflow-survey-456106")
    # replace if not using env var
    bq_project = os.getenv("GCP_PROJECT", "stackoverflow-survey-456106")
    bq_dataset = "stackoverflow_survey_dataset"

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("StackOverflowSurveyCleaner") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
        .getOrCreate()

    # Initialize GCS client
    storage_client = storage.Client()

    for year in range(start_year, end_year + 1):
        source_blob_name = f"cleaned_csv/{year}-survey.csv"
        local_path = f"/tmp/{year}-survey.csv"

        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)

            if not blob.exists():
                print(f"File not found in GCS for year {year}, skipping.")
                continue

            blob.download_to_filename(local_path)
            print(f"Downloaded {source_blob_name} to {local_path}")

            # df = spark.read.option("header", "true").option(
            #     "inferSchema", "true").csv(local_path)

            df = spark.read.csv(local_path, header=True, inferSchema=True)
            print(f"This is the dataframe schema: {df.schema}")
            print(f"This is the dataframe: {df.show()}")

            # Apply year-specific transformation
            cleaned_df = apply_transformation(df, year)

            # Upload to BigQuery
            bq_table = f"{bq_dataset}.{year}_survey"

            cleaned_df.write \
                .format("bigquery") \
                .option("table", bq_table) \
                .option("temporaryGcsBucket", bucket_name) \
                .option("writeMethod", "direct") \
                .mode("overwrite") \
                .save()

            print(f"Uploaded to BigQuery: {bq_table}")
            os.remove(local_path)

        except Exception as e:
            print(f"Error processing year {year}: {e}")

    spark.stop()
