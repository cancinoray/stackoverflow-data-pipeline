from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, concat_ws, monotonically_increasing_id
from pyspark.sql import types


def transform_2011(df):
    # selecting columns to be used
    df_2011_raw = df.select(
        "What Country or Region do you live in?",
        "How old are you?",
        "How many years of IT/Programming experience do you have?",
        "Which of the following best describes your occupation?",
        "Which languages are you proficient in?",
        "_c31",
        "_c32",
        "_c33",
        "_c34",
        "_c35",
        "_c36",
        "_c37",
        "_c38",
        "_c39",
        "_c40",
        "_c41",
        "_c42",
        "What operating system do you use the most?",
        "Please rate your job/career satisfaction",
        "Including bonus, what is your annual compensation in USD?",
        "Which technology products do you own? (You can choose more than one)",
        "_c47",
        "_c48",
        "_c49",
        "_c50",
        "_c51",
        "_c52",
        "_c53",
        "_c54",
        "_c55",
        "_c56",
        "_c57",
        "_c58",
        "_c59",
        "_c60",
        "_c61",
        "_c62"
    )

    # Concatenating the columns for programming languages
    # List all the relevant columns
    columns = [
        "Which languages are you proficient in?",
        "_c31", "_c32", "_c33", "_c34", "_c35", "_c36",
        "_c37", "_c38", "_c39", "_c40", "_c41", "_c42"
    ]

    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]
    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2011_raw = df_2011_raw.withColumn(
        "prog_language_proficient_in", concat_ws(", ", *cleaned_cols))
    # Drop the original columns
    df_2011_raw = df_2011_raw.drop(*columns)

    # Concatenating the columns for technology own
    # List all the relevant columns
    columns = [
        "Which technology products do you own? (You can choose more than one)",
        "_c47", "_c48", "_c49", "_c50", "_c51", "_c52", "_c53", "_c54", "_c55", "_c56", "_c57", "_c58", "_c59", "_c60", "_c61", "_c62",
    ]
    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]
    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2011_raw = df_2011_raw.withColumn(
        "tech_own", concat_ws(", ", *cleaned_cols))
    # Drop the original columns
    df_2011_raw = df_2011_raw.drop(*columns)

    # Selecting the columns to be used
    df_2011_raw = df_2011_raw.select(
        col("What Country or Region do you live in?").alias("country"),
        col("How old are you?").alias("age"),
        col("How many years of IT/Programming experience do you have?").alias("experience_years"),
        col("Which of the following best describes your occupation?").alias(
            "occupation"),
        col("What operating system do you use the most?").alias("os_used"),
        col("Please rate your job/career satisfaction").alias("job_satisfaction"),
        col("Including bonus, what is your annual compensation in USD?").alias(
            "annual_compensation"),
        col("prog_language_proficient_in").alias(
            "prog_language_proficient_in"),
        col("tech_own").alias("tech_own")
    )

    # Removing the first row
    # Add index
    df_2011_raw = df_2011_raw.withColumn(
        'index', monotonically_increasing_id())
    # remove rows by filtering
    df_2011_raw = df_2011_raw.filter(~df_2011_raw.index.isin(0))
    # drop the index column
    df_2011_raw = df_2011_raw.drop("index")

    # Adding new columns
    # adding year column
    df_2011_raw = df_2011_raw.withColumn("year", lit("2011"))
    # adding new columns
    new_columns = ["sex", "education", "prog_language_desired"]
    for col_name in new_columns:
        df_2011_raw = df_2011_raw.withColumn(col_name, lit(None))

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
    df_2011 = df_2011_raw.select(*reordered_columns)

    # Schema validation and editing
    df_2011 = df_2011.withColumn("year", col("year").cast(types.IntegerType())) \
        .withColumn("sex", col("sex").cast("string")) \
        .withColumn("education", col("education").cast("string")) \
        .withColumn("prog_language_desired", col("prog_language_desired").cast("string"))

    # Cleaning and Transforming the data
    # cleaning experience_years column
    df_2011 = df_2011.withColumn(
        "experience_years",
        when(col("experience_years") == "11", "11+")
        .when(col("experience_years") == "41435", "6-10")
        .when(col("experience_years") == "41310", "2-5")
        .when(col("experience_years") == "<2", "<2")
        .otherwise(None)  # or keep as is with col("experience_years")
    )

    # cleaning the annual_compensation column
    df_2011 = df_2011.withColumn(
        "annual_compensation_usd",
        when(col("annual_compensation") == "Student / Unemployed", "0")
        .otherwise(regexp_replace(col("annual_compensation"), r"\$", ""))
    )
    # Drop the original column
    df_2011 = df_2011.drop("annual_compensation")

    # cleaning the os_used column
    df_2011 = df_2011.withColumn(
        "os_used",
        when(col("os_used").isNull(), None)
        .when(col("os_used").like("Windows%"), "Windows")
        .when(col("os_used").like("Linux"), "Linux")
        .when(col("os_used").like("Mac OS%"), "Mac OS")
        .otherwise(None)
    )

    df_2011.show()
    return df_2011
