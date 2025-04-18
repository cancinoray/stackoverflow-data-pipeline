from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, concat_ws, monotonically_increasing_id
from pyspark.sql import types


def transform_2020(df):
    df_2020_raw = df.select(
        "Country",
        "EdLevel",
        "UndergradMajor",
        "DevType",
        "YearsCode",
        "JobSat",
        "ConvertedComp",
        "LanguageWorkedWith",
        "LanguageDesireNextYear",
        "OpSys",
        "Age",
        "Gender",
    )

    df_2020_raw.show()

    # List all the relevant columns
    columns = [
        "EdLevel",
        "UndergradMajor",
    ]

    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (col(c) != ""), col(c))
                    for c in columns]

    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2020_raw = df_2020_raw.withColumn(
        "education", concat_ws(", ", *cleaned_cols))

    # Drop the original columns
    df_2020_raw = df_2020_raw.drop(*columns)

    df_2020_raw.show()

    df_2020_raw = df_2020_raw.select(
        col("Country").alias("country"),
        col("Age").alias("age"),
        col("Gender").alias("sex"),
        col("DevType").alias("occupation"),
        col("YearsCode").alias("experience_years"),
        col("ConvertedComp").alias("annual_compensation"),
        col("LanguageWorkedWith").alias("prog_language_proficient_in"),
        col("LanguageWorkedWith").alias("prog_language_desired"),
        col("JobSat").alias("job_satisfaction"),
        col("OpSys").alias("os_used"),
        col("education").alias("education"),
    )

    df_2020_raw.show()

    # adding year column
    df_2020_raw = df_2020_raw.withColumn("year", lit("2020"))

    # adding new columns
    new_columns = ["tech_own"]

    for col_name in new_columns:
        df_2020_raw = df_2020_raw.withColumn(col_name, lit(None))

    # Reorder columns
    reordered_columns = [
        "year",
        "country",
        "sex",
        "age",
        "education",
        "occupation",
        "experience_years",
        "annual_compensation",
        "job_satisfaction",
        "tech_own",
        "os_used",
        "prog_language_proficient_in",
        "prog_language_desired"
    ]

    df_2020 = df_2020_raw.select(*reordered_columns)

    df_2020.show()

    df_2020.printSchema()

    # schema validation and editing
    df_2020 = df_2020.withColumn("year", col("year").cast(types.IntegerType())) \
        .withColumn("tech_own", col("tech_own").cast("string")) \

    df_2020.printSchema()

    df_2020.groupBy("experience_years").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2020 = df_2020.withColumn(
        "experience_years",
        when(col("experience_years").cast("int") < 2, "<2")
        .when((col("experience_years").cast("int") >= 2) & (col("experience_years").cast("int") <= 5), "2-5")
        .when((col("experience_years").cast("int") >= 6) & (col("experience_years").cast("int") <= 10), "6-10")
        .when(col("experience_years").cast("int") > 10, "11+")
        .otherwise(None)
    )

    df_2020.groupBy("experience_years").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2020.groupBy("age").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2020 = df_2020.withColumn(
        "age",
        when(col("age").isNull(), "null")
        .when(col("age") < 20, "< 20")
        .when((col("age") >= 20) & (col("age") <= 24), "20-24")
        .when((col("age") >= 25) & (col("age") <= 29), "25-29")
        .when((col("age") >= 30) & (col("age") <= 34), "30-34")
        .when((col("age") >= 35) & (col("age") <= 39), "35-39")
        .when((col("age") >= 40) & (col("age") <= 50), "40-50")
        .when((col("age") >= 51) & (col("age") <= 60), "51-60")
        .otherwise(">60")
    )

    df_2020.groupBy("age").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2020.show()

    df_2020 = df_2020.withColumn(
        "sex",
        when(col("sex") == "Man", "Male")
        .when(col("sex") == "Woman", "Female")
        .otherwise(None)
    )

    df_2020.groupBy("sex").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2020 = df_2020.withColumn(
        "annual_compensation_usd",
        when(col("annual_compensation").isNull(), None)
        .when(col("annual_compensation") == 0, "0")
        .when(col("annual_compensation") < 10000, "<10,000")
        .when((col("annual_compensation") >= 10000) & (col("annual_compensation") < 20000), "10,000 - 20,000")
        .when((col("annual_compensation") >= 20000) & (col("annual_compensation") < 30000), "20,000 - 30,000")
        .when((col("annual_compensation") >= 30000) & (col("annual_compensation") < 40000), "30,000 - 40,000")
        .when((col("annual_compensation") >= 40000) & (col("annual_compensation") < 50000), "40,000 - 50,000")
        .when((col("annual_compensation") >= 50000) & (col("annual_compensation") < 60000), "50,000 - 60,000")
        .when((col("annual_compensation") >= 60000) & (col("annual_compensation") < 70000), "60,000 - 70,000")
        .when((col("annual_compensation") >= 70000) & (col("annual_compensation") < 80000), "70,000 - 80,000")
        .when((col("annual_compensation") >= 80000) & (col("annual_compensation") < 90000), "80,000 - 90,000")
        .when((col("annual_compensation") >= 90000) & (col("annual_compensation") < 100000), "90,000 - 100,000")
        .when((col("annual_compensation") >= 100000) & (col("annual_compensation") < 110000), "100,000 - 110,000")
        .when((col("annual_compensation") >= 110000) & (col("annual_compensation") < 120000), "110,000 - 120,000")
        .when((col("annual_compensation") >= 120000) & (col("annual_compensation") < 130000), "120,000 - 130,000")
        .when((col("annual_compensation") >= 130000) & (col("annual_compensation") < 140000), "130,000 - 140,000")
        .when((col("annual_compensation") >= 140000) & (col("annual_compensation") < 150000), "140,000 - 150,000")
        .when((col("annual_compensation") >= 150000) & (col("annual_compensation") < 160000), "150,000 - 160,000")
        .when((col("annual_compensation") >= 160000) & (col("annual_compensation") < 170000), "160,000 - 170,000")
        .when(col("annual_compensation") > 200000, ">200,000")
        .otherwise(None)
    )

    # Drop the original column
    df_2020 = df_2020 .drop("annual_compensation")

    df_2020.groupBy("annual_compensation_usd").count().show(truncate=False)

    df_2020.show(5, truncate=False)
    return df_2020
