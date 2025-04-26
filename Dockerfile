FROM quay.io/astronomer/astro-runtime:12.7.1

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME for amd64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_SUBMIT_ARGS="--packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 pyspark-shell"


USER astro
