from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, concat_ws, monotonically_increasing_id
from pyspark.sql import types


def transform_2017(df):
    df_2017_raw = df.select(
        "Country",
        "YearsProgram",
        "DeveloperType",
        "WebDeveloperType",
        "MobileDeveloperType",
        "JobSatisfaction",
        "FormalEducation",
        "SelfTaughtTypes",
        "WantWorkLanguage",
        "HaveWorkedFramework",
        "Overpaid",
        "StackOverflowFoundAnswer",
        "ExpectedSalary",
        "HighestEducationParents",
        "StackOverflowFoundAnswer"

    )

    df_2017_raw.show()

    # List all the relevant columns
    columns = [
        "DeveloperType",
        "WebDeveloperType",
        "MobileDeveloperType"
    ]

    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (col(c) != ""), col(c))
                    for c in columns]

    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2017_raw = df_2017_raw.withColumn(
        "occupation", concat_ws(", ", *cleaned_cols))

    # Drop the original columns
    df_2017_raw = df_2017_raw.drop(*columns)

    df_2017_raw.show()

    # List all the relevant columns
    columns = [
        "FormalEducation",
        "SelfTaughtTypes",
    ]

    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (col(c) != ""), col(c))
                    for c in columns]

    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2017_raw = df_2017_raw.withColumn(
        "education", concat_ws(", ", *cleaned_cols))

    # Drop the original columns
    df_2017_raw = df_2017_raw.drop(*columns)

    df_2017_raw.show()

    df_2017_raw.groupBy("Overpaid").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2017_raw = df_2017_raw.withColumn(
        "ExpectedSalary",
        when(col("Overpaid") == "U.S. dollars ($)",
             col("ExpectedSalary").cast("double"))
        .otherwise(lit(None).cast("double"))
    )

    df_2017_raw.select("Overpaid", "ExpectedSalary").show()

    df_2017_raw.printSchema()

    df_2017_raw.groupBy("ExpectedSalary").count().show(truncate=False)

    df_2017_raw = df_2017_raw.withColumn(
        "annual_compensation_usd",
        when(col("ExpectedSalary").isNull(), None)
        .when(col("ExpectedSalary") == 0, "0")
        .when(col("ExpectedSalary") < 10000, "<10,000")
        .when((col("ExpectedSalary") >= 10000) & (col("ExpectedSalary") < 20000), "10,000 - 20,000")
        .when((col("ExpectedSalary") >= 20000) & (col("ExpectedSalary") < 30000), "20,000 - 30,000")
        .when((col("ExpectedSalary") >= 30000) & (col("ExpectedSalary") < 40000), "30,000 - 40,000")
        .when((col("ExpectedSalary") >= 40000) & (col("ExpectedSalary") < 50000), "40,000 - 50,000")
        .when((col("ExpectedSalary") >= 50000) & (col("ExpectedSalary") < 60000), "50,000 - 60,000")
        .when((col("ExpectedSalary") >= 60000) & (col("ExpectedSalary") < 70000), "60,000 - 70,000")
        .when((col("ExpectedSalary") >= 70000) & (col("ExpectedSalary") < 80000), "70,000 - 80,000")
        .when((col("ExpectedSalary") >= 80000) & (col("ExpectedSalary") < 90000), "80,000 - 90,000")
        .when((col("ExpectedSalary") >= 90000) & (col("ExpectedSalary") < 100000), "90,000 - 100,000")
        .when((col("ExpectedSalary") >= 100000) & (col("ExpectedSalary") < 110000), "100,000 - 110,000")
        .when((col("ExpectedSalary") >= 110000) & (col("ExpectedSalary") < 120000), "110,000 - 120,000")
        .when((col("ExpectedSalary") >= 120000) & (col("ExpectedSalary") < 130000), "120,000 - 130,000")
        .when((col("ExpectedSalary") >= 130000) & (col("ExpectedSalary") < 140000), "130,000 - 140,000")
        .when((col("ExpectedSalary") >= 140000) & (col("ExpectedSalary") < 150000), "140,000 - 150,000")
        .when((col("ExpectedSalary") >= 150000) & (col("ExpectedSalary") < 160000), "150,000 - 160,000")
        .when((col("ExpectedSalary") >= 160000) & (col("ExpectedSalary") < 170000), "160,000 - 170,000")
        .when(col("ExpectedSalary") > 200000, ">200,000")
        .otherwise(None)
    )

    # Drop the original column
    df_2017_raw = df_2017_raw .drop("ExpectedSalary")
    df_2017_raw = df_2017_raw .drop("Overpaid")

    df_2017_raw.groupBy("annual_compensation_usd").count().show(truncate=False)

    df_2017_raw.show()

    df_2017_raw = df_2017_raw.select(
        col("Country").alias("country"),
        col("HighestEducationParents").alias("sex"),
        col("occupation").alias("occupation"),
        col("YearsProgram").alias("experience_years"),
        col("annual_compensation_usd").alias("annual_compensation_usd"),
        col("WantWorkLanguage").alias("prog_language_proficient_in"),
        col("HaveWorkedFramework").alias("prog_language_desired"),
        col("JobSatisfaction").alias("job_satisfaction"),
        col("StackOverflowFoundAnswer").alias("tech_own"),
        col("education").alias("education"),
    )

    df_2017_raw.show()

    # adding year column
    df_2017_raw = df_2017_raw.withColumn("year", lit("2017"))

    # adding new columns
    new_columns = ["os_used", "age"]

    for col_name in new_columns:
        df_2017_raw = df_2017_raw.withColumn(col_name, lit(None))

    df_2017_raw.show()

    # Reorder columns
    reordered_columns = [
        "year",
        "country",
        "sex",
        "age",
        "education",
        "occupation",
        "experience_years",
        "annual_compensation_usd",
        "job_satisfaction",
        "tech_own",
        "os_used",
        "prog_language_proficient_in",
        "prog_language_desired"
    ]

    df_2017 = df_2017_raw.select(*reordered_columns)

    df_2017.show()

    df_2017.printSchema()

    # schema validation and editing
    df_2017 = df_2017.withColumn("year", col("year").cast(types.IntegerType())) \
        .withColumn("age", col("age").cast("string")) \
        .withColumn("os_used", col("os_used").cast("string"))

    df_2017.groupBy("experience_years").count().show(truncate=False)

    # cleaning experience_years column
    df_2017 = df_2017.withColumn(
        "experience_years",
        when(col("experience_years").isin(
            "Less than a year", "1 to 2 years", "2 to 3 years"), "<2")
        .when(col("experience_years").isin("3 to 4 years", "4 to 5 years", "5 to 6 years"), "2-5")
        .when(col("experience_years").isin("6 to 7 years", "7 to 8 years", "8 to 9 years", "9 to 10 years"), "6-10")
        .when(col("experience_years").isin("10 to 11 years", "12 to 13 years", "13 to 14 years",
                                           "14 to 15 years", "15 to 16 years", "17 to 18 years",
                                           "18 to 19 years", "19 to 20 years", "20 or more years"), "11+")
        .otherwise("NA")
    )

    df_2017.groupBy("experience_years").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2017.show(5, truncate=False)
    return df_2017
