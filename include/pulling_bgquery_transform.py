from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from functools import reduce
import os


def combine_surveys(start_year: int = 2011, end_year: int = 2024,):
    project_id = os.getenv("GCP_PROJECT", "stackoverflow-survey-456106")
    dataset = os.getenv("GCP_BGQUERY", "stackoverflow_survey_dataset")
    bucket_name = os.getenv(
        "GCS_BUCKET", "gsbucket-stackoverflow-survey-456106")
    output_table = "stackoverflow_survey_2011_2024_combined"

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Combine Stack Overflow Surveys") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
        .getOrCreate()

    # Load and tag each year's survey with error handling
    def load_survey(year):
        table = f"{project_id}.{dataset}.{year}_survey"
        try:
            df = spark.read.format("bigquery").option("table", table).load()
            return df.withColumn("survey_year", lit(year))
        except Exception as e:
            print(f"Error loading survey for year {year}: {str(e)}")
            return None

    # Process all years
    years = list(range(start_year, end_year + 1))
    dfs = [df for df in (load_survey(year)
                         for year in years) if df is not None]

    if not dfs:
        print("No survey data was successfully loaded. Please check table names.")
        return

    # Combine all into one DataFrame
    combined_df = reduce(lambda a, b: a.unionByName(
        b, allowMissingColumns=True), dfs)
    print(f"Total combined rows survey: {combined_df.count()}")

    # Write the result to BigQuery
    combined_df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset}.{output_table}") \
        .option("temporaryGcsBucket", bucket_name) \
        .option("writeMethod", "direct") \
        .mode("overwrite") \
        .save()

    print(
        f"✅ Combined surveys from {start_year} to {end_year} written to {output_table} in BigQuery!")
