from google.cloud import storage, bigquery
import os
import re
import zipfile
import tempfile
import io
import csv


def normalize_headers_with_dedup(headers):
    seen = {}
    normalized = []
    for h in headers:
        base = re.sub(r'\W+', '_', h.strip().lower())
        if base in seen:
            seen[base] += 1
            normalized.append(f"{base}_{seen[base]}")
        else:
            seen[base] = 1
            normalized.append(base)
    return normalized


def extract_upload_csvs_to_gcs_and_bigquery():
    bucket_name = os.getenv(
        "GCS_BUCKET", "gsbucket-stackoverflow-survey-456106")

    storage_client = storage.Client()
    bigquery_client = bigquery.Client()
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        if blob.name.endswith(".zip"):
            zip_filename = os.path.basename(blob.name)
            print(f"Processing: {zip_filename}")

            # Extract year from filename
            match = re.search(r'(20\d{2})', zip_filename)
            year = match.group(1) if match else "unknown"

            # Download zip to memory
            zip_content = io.BytesIO()
            blob.download_to_file(zip_content)
            zip_content.seek(0)
            print(f"Downloaded: {zip_filename}")

            with tempfile.TemporaryDirectory() as temp_dir:
                with zipfile.ZipFile(zip_content, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)

                # Find all CSVs
                csv_files = []
                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        if file.endswith(".csv"):
                            csv_files.append(os.path.join(root, file))

                if not csv_files:
                    print(f"No CSV found in {zip_filename}")
                    continue

                largest_csv = max(csv_files, key=os.path.getsize)
                cleaned_csv_path = os.path.join(
                    temp_dir, f"{year}_cleaned.csv")

                # Clean and normalize
                with open(largest_csv, 'r', encoding='utf-8', errors='ignore') as infile, \
                        open(cleaned_csv_path, 'w', newline='', encoding='utf-8') as outfile:

                    reader = csv.reader(infile)
                    writer = csv.writer(outfile)

                    headers = next(reader)
                    normalized_headers = [
                        'id'] + [normalize_headers_with_dedup for h in headers] + ['year']
                    writer.writerow(normalized_headers)

                    for idx, row in enumerate(reader, start=1):
                        row_data = [idx] + row + [year]
                        writer.writerow(row_data)

                # Upload cleaned file to GCS
                destination_blob = f"cleaned_csv/{year}-survey.csv"
                upload_blob = storage_client.bucket(
                    bucket_name).blob(destination_blob)
                upload_blob.upload_from_filename(cleaned_csv_path)
                print(
                    f"Uploaded to GCS: gs://{bucket_name}/{destination_blob}")

                # Upload to BigQuery
                table_id = f"{bigquery_client.project}.stackoverflow_survey_dataset.survey_{year}"
                job_config = bigquery.LoadJobConfig(
                    autodetect=True,
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                )

                with open(cleaned_csv_path, "rb") as source_file:
                    load_job = bigquery_client.load_table_from_file(
                        source_file,
                        table_id,
                        job_config=job_config,
                    )
                    load_job.result()
                    print(f"Uploaded to BigQuery: {table_id}")
