from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, concat_ws, monotonically_increasing_id, split, lower
from pyspark.sql import types


def transform_2018(df):
    df_2018_raw = df.select(
        "Country",
        "FormalEducation",
        "DevType",
        "YearsCoding",
        "JobSatisfaction",
        "ConvertedSalary",
        "LanguageWorkedWith",
        "LanguageDesireNextYear",
        "OperatingSystem",
        "Gender",
        "Age",
    )

    df_2018_raw = df_2018_raw.withColumn(
        "DevType", split(col("DevType"), ";")[0])

    df_2018_raw = df_2018_raw.select(
        col("Country").alias("country"),
        col("Age").alias("age"),
        col("Gender").alias("sex"),
        col("DevType").alias("occupation"),
        col("YearsCoding").alias("experience_years"),
        col("ConvertedSalary").alias("annual_compensation"),
        col("LanguageWorkedWith").alias("prog_language_proficient_in"),
        col("LanguageDesireNextYear").alias("prog_language_desired"),
        col("JobSatisfaction").alias("job_satisfaction"),
        col("OperatingSystem").alias("os_used"),
        col("FormalEducation").alias("education"),
    )

    # adding year column
    df_2018_raw = df_2018_raw.withColumn("year", lit("2018"))
    # adding new columns
    new_columns = ["tech_own"]
    for col_name in new_columns:
        df_2018_raw = df_2018_raw.withColumn(col_name, lit(None))

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

    df_2018 = df_2018_raw.select(*reordered_columns)

    # schema validation and editing
    df_2018 = df_2018.withColumn("year", col("year").cast(types.IntegerType())) \
        .withColumn("tech_own", col("tech_own").cast("string")) \


    df_2018 = df_2018.withColumn(
        "experience_years",
        when(col("experience_years") == "0-2 years", "<2")
        .when(col("experience_years") == "3-5 years", "2-5")
        .when(col("experience_years").isin("6-8 years", "9-11 years"), "6-10")
        .when(col("experience_years").isin(
            "12-14 years", "15-17 years", "18-20 years",
            "21-23 years", "24-26 years", "27-29 years", "30 or more years"
        ), "11+")
        .otherwise(None)
    )

    df_2018 = df_2018.withColumn(
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
    df_2018 = df_2018 .drop("annual_compensation")
    df_2018.groupBy("annual_compensation_usd").count().show(truncate=False)

    df_2018 = df_2018.withColumn(
        "age",
        when(col("age") == "55 - 64 years old", "55-64")
        .when(col("age") == "45 - 54 years old", "45-54")
        .when(col("age") == "18 - 24 years old", "18-24")
        .when(col("age") == "35 - 44 years old", "35-44")
        .when(col("age") == "25 - 34 years old", "25-34")
        .when(col("age") == "65 years or older", ">65")
        .when(col("age") == "Under 18 years old", "<18")
        .otherwise(None)
    )

    df_2018 = df_2018.withColumn(
        "os_used",
        when(col("os_used") == "Windows", "Windows")
        .when(col("os_used") == "Linux-based", "Linux")
        .when(col("os_used") == "MacOS", "Mac OS")
        .when(col("os_used") == "NA", None)
        .otherwise(None)
    )

    df_2018 = df_2018.withColumn(
        "sex",
        when(col("sex").isNull(), "Unknown")
        .when(lower(col("sex")).isin("prefer not to disclose", "i prefer not to answer", "i don't know/not sure"), "Unknown")
        .when(lower(col("sex")).rlike("degree|high schoocollege|primary|no education"), "Unknown")
        .when(lower(col("sex")) == "male", "Male")
        .when(lower(col("sex")) == "female", "Female")
        .when(lower(col("sex")).contains("non-binary"), "Non-binary")
        .when(lower(col("sex")).contains("genderqueer"), "Non-binary")
        .when(lower(col("sex")).contains("gender non-conforming"), "Non-binary")
        .when(lower(col("sex")).contains("transgender"), "Transgender")
        .when(lower(col("sex")).contains("other"), "Other")
        .when(col("sex").contains(";"), "Multiple")
        .otherwise("Other")
    )

    df_2018.show()
    return df_2018
