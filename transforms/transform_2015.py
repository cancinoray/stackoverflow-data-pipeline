from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, concat_ws, monotonically_increasing_id
from pyspark.sql import types


def transform_2015(df):
    df_2015_raw = df.select(
        "_c0", "_c1", "_c2", "_c4", "_c5", "_c6", "Select all that apply8",
        "_c9", "_c10", "_c11", "_c12", "_c13", "_c14", "_c15", "_c16", "_c17", "_c18", "_c19",
        "_c20", "_c21", "_c22", "_c23", "_c24", "_c25", "_c26", "_c27", "_c28", "_c29", "_c30",
        "_c31", "_c32", "_c33", "_c34", "_c35", "_c36", "_c37", "_c38", "_c39", "_c40", "_c41",
        "_c42", "_c43", "_c44", "_c45", "_c46", "_c47", "_c48", "_c49", "_c50", "Select all that apply51",
        "_c52", "_c53", "_c54", "_c55", "_c56", "_c57", "_c58", "_c59", "_c60", "_c61", "_c62",
        "_c63", "_c64", "_c65", "_c66", "_c67", "_c68", "_c69", "_c70", "_c71", "_c72", "_c73",
        "_c74", "_c75", "_c76", "_c77", "_c78", "_c79", "_c80", "_c81", "_c82", "_c83", "_c84",
        "_c85", "_c86", "_c87", "_c88", "_c89", "_c90", "_c91", "_c92", "_c93", "Select all that apply94",
        "_c95", "_c96", "_c97", "_c98", "_c99", "_c100", "_c101", "_c102", "_c103", "_c104",
        "_c105", "_c109"
    )

    # List of columns representing programming language selections
    columns = [
        "Select all that apply8",
        "_c9",
        "_c48"
    ]

    # Replace nulls or empty strings with None, keeping valid values
    cleaned_language_cols = [
        when((col(c).isNotNull()) & (col(c) != ""), col(c)) for c in columns
    ]
    # Concatenate non-null values into a single column
    df_2015_raw = df_2015_raw.withColumn(
        "tech_own", concat_ws(", ", *cleaned_language_cols)
    )
    # Drop the original programming language columns
    df_2015_raw = df_2015_raw.drop(*columns)

    # List of columns representing programming language selections
    columns = [
        "_c10", "_c11", "_c12", "_c13", "_c14", "_c15", "_c16", "_c17", "_c18", "_c19",
        "_c20", "_c21", "_c22", "_c23", "_c24", "_c25", "_c26", "_c27", "_c28", "_c29",
        "_c30", "_c31", "_c32", "_c33", "_c34", "_c35", "_c36", "_c37", "_c38", "_c39",
        "_c40", "_c41", "_c42", "_c43", "_c44", "_c45", "_c46", "_c47", "_c49", "_c50"
    ]
    # Replace nulls or empty strings with None, keeping valid values
    cleaned_language_cols = [
        when((col(c).isNotNull()) & (col(c) != ""), col(c)) for c in columns
    ]
    # Concatenate non-null values into a single column
    df_2015_raw = df_2015_raw.withColumn(
        "prog_language_proficient_in", concat_ws(", ", *cleaned_language_cols)
    )
    # Drop the original programming language columns
    df_2015_raw = df_2015_raw.drop(*columns)

    # Concatenating the columns for programming languages preferred
    # List all the relevant columns
    columns = [
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
        "_c63",
        "_c64",
        "_c65",
        "_c66",
        "_c67",
        "_c68",
        "_c69",
        "_c70",
        "_c71",
        "_c72",
        "_c73",
        "_c74",
        "_c75",
        "_c76",
        "_c77",
        "_c78",
        "_c79",
        "_c80",
        "_c81",
        "_c82",
        "_c83",
        "_c84",
        "_c85",
        "_c86",
        "_c87",
        "_c88",
        "_c89",
        "_c90",
        "_c92",
        "_c93",
    ]
    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]
    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2015_raw = df_2015_raw.withColumn(
        "prog_language_desired", concat_ws(", ", *cleaned_cols))
    # Drop the original columns
    df_2015_raw = df_2015_raw.drop(*columns)

    # Concatenating the columns for programming languages preferred
    # List all the relevant columns
    columns = [
        "Select all that apply94",
        "_c95",
        "_c96",
        "_c97",
        "_c98",
        "_c99",
        "_c100",
        "_c101",
        "_c102",
        "_c103",
        "_c104",
    ]
    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]
    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2015_raw = df_2015_raw.withColumn(
        "education", concat_ws(", ", *cleaned_cols))
    # Drop the original columns
    df_2015_raw = df_2015_raw.drop(*columns)

    # Selecting the relevant columns
    df_2015_raw = df_2015_raw.select(
        col("_c0").alias("country"),
        col("_c1").alias("age"),
        col("_c2").alias("sex"),
        col("_c4").alias("experience_years"),
        col("_c5").alias("occupation"),
        col("_c6").alias("os_used"),
        col("_c105").alias("annual_compensation"),
        col("_c109").alias("job_satisfaction"),
        col("education").alias("education"),
        col("tech_own").alias("tech_own"),
        col("prog_language_proficient_in").alias(
            "prog_language_proficient_in"),
        col("prog_language_desired").alias("prog_language_desired")
    )

    # Removing the first row
    # Add index
    df_2015_raw = df_2015_raw.withColumn(
        'index', monotonically_increasing_id())
    # remove rows by filtering
    df_2015_raw = df_2015_raw.filter(~df_2015_raw.index.isin(0))
    # drop the index column
    df_2015_raw = df_2015_raw.drop("index")

    # adding year column
    df_2015_raw = df_2015_raw.withColumn("year", lit("2015"))

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
    df_2015 = df_2015_raw.select(*reordered_columns)

    # cleaning experience_years column
    df_2015 = df_2015.withColumn(
        "experience_years",
        when(col("experience_years") == "11+ years", "11+")
        .when(col("experience_years") == "2 - 5 years", "2-5")
        .when(col("experience_years") == "6 - 10 years", "6-10")
        .when(col("experience_years") == "1 - 2 years", "<2")
        .when(col("experience_years") == "Less than 1 year", "<2")
        .otherwise(None)
    )

    # Clean and create the new column
    df_2015 = df_2015.withColumn(
        "annual_compensation_usd",
        when(col("annual_compensation") == "Unemployed", "0")
        .when(col("annual_compensation") == "Rather not say", None)
        .when(col("annual_compensation") == "More than $160,000", ">160,000")
        .when(col("annual_compensation") == "Less than $20,000", "<20,000")
        .otherwise(regexp_replace(col("annual_compensation"), r"\$", ""))
    )

    # Drop the original column
    df_2015 = df_2015.drop("annual_compensation")

    df_2015.show()
    return df_2015
