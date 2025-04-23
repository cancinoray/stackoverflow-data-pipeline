from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, concat_ws, monotonically_increasing_id
from pyspark.sql import types


def transform_2012(df):
    df_2012_raw = df.select(
        "What Country or Region do you live in?",
        "How old are you?",
        "How many years of IT/Programming experience do you have?",
        "Which of the following best describes your occupation?",
        "Which languages are you proficient in?",
        "_c23",
        "_c24",
        "_c25",
        "_c26",
        "_c27",
        "_c28",
        "_c29",
        "_c30",
        "_c31",
        "_c32",
        "_c33",
        "_c34",
        "_c35",
        "_c36",
        "Which desktop operating system do you use the most?",
        "What best describes your career / job satisfaction? ",
        "Including bonus, what is your annual compensation in USD?",
        "Which technology products do you own? (You can choose more than one)",
        "_c45",
        "_c46",
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
        "_c62",
        "_c63"
    )

    # Concatenating the columns
    # List all the relevant columns
    columns = [
        "Which languages are you proficient in?",
        "_c23",
        "_c24",
        "_c25",
        "_c26",
        "_c27",
        "_c28",
        "_c29",
        "_c30",
        "_c31",
        "_c32",
        "_c33",
        "_c34",
        "_c35",
        "_c36",
    ]

    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]
    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2012_raw = df_2012_raw.withColumn(
        "prog_language_proficient_in", concat_ws(", ", *cleaned_cols))
    # Drop the original columns
    df_2012_raw = df_2012_raw.drop(*columns)

    # List all the relevant columns
    columns = [
        "Which technology products do you own? (You can choose more than one)",
        "_c45",
        "_c46",
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
        "_c62",
        "_c63"
    ]
    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]
    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2012_raw = df_2012_raw.withColumn(
        "tech_own", concat_ws(", ", *cleaned_cols))
    # Drop the original columns
    df_2012_raw = df_2012_raw.drop(*columns)

    # Selecting relevant columns
    df_2012_raw = df_2012_raw.select(
        col("What Country or Region do you live in?").alias("country"),
        col("How old are you?").alias("age"),
        col("How many years of IT/Programming experience do you have?").alias("experience_years"),
        col("Which of the following best describes your occupation?").alias(
            "occupation"),
        col("Which desktop operating system do you use the most?").alias("os_used"),
        col("What best describes your career / job satisfaction? ").alias("job_satisfaction"),
        col("Including bonus, what is your annual compensation in USD?").alias(
            "annual_compensation"),
        col("prog_language_proficient_in").alias(
            "prog_language_proficient_in"),
        col("tech_own").alias("tech_own")
    )

    # Removing the first row
    # Add index
    df_2012_raw = df_2012_raw.withColumn(
        'index', monotonically_increasing_id())
    # remove rows by filtering
    df_2012_raw = df_2012_raw.filter(~df_2012_raw.index.isin(0))
    # drop the index column
    df_2012_raw = df_2012_raw.drop("index")

    # Adding new columns
    # adding year column
    df_2012_raw = df_2012_raw.withColumn("year", lit("2012"))
    # adding new columns
    new_columns = ["sex", "education", "prog_language_desired"]
    for col_name in new_columns:
        df_2012_raw = df_2012_raw.withColumn(col_name, lit(None))

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
    df_2012 = df_2012_raw.select(*reordered_columns)

    # schema validation and editing
    df_2012 = df_2012.withColumn("year", col("year").cast(types.IntegerType())) \
        .withColumn("sex", col("sex").cast("string")) \
        .withColumn("education", col("education").cast("string")) \
        .withColumn("prog_language_desired", col("prog_language_desired").cast("string"))

    # cleaning experience_years column
    df_2012 = df_2012.withColumn(
        "experience_years",
        when(col("experience_years") == "11", "11+")
        .when(col("experience_years") == "40944", "6-10")
        .when(col("experience_years") == "41070", "2-5")
        .when(col("experience_years") == "<2", "<2")
        .otherwise(None)  # or keep as is with col("experience_years")
    )

    # Cleaning and Transforming the data
    df_2012 = df_2012.withColumn(
        "annual_compensation_usd",
        when(col("annual_compensation") == "Student / Unemployed", "0")
        .when(col("annual_compensation") == "Rather not say", None)
        .otherwise(regexp_replace(col("annual_compensation"), r"\$", ""))
    )
    # Drop the original column
    df_2012 = df_2012.drop("annual_compensation")

    # cleaning the os_used column
    df_2012 = df_2012.withColumn(
        "os_used",
        when(col("os_used").isNull(), None)
        .when(col("os_used").like("Windows%"), "Windows")
        .when(col("os_used").like("Linux"), "Linux")
        .when(col("os_used").like("Mac OS%"), "Mac OS")
        .otherwise(None)
    )

    df_2012.show()
    return df_2012
