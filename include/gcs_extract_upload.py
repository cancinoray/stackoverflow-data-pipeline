from google.cloud import storage, bigquery
import os
import re
import zipfile
import tempfile
import io
import shutil


def extract_upload_csvs_to_gcs_and_bigquery():
    bucket_name = os.getenv(
        "GCS_BUCKET", "gsbucket-stackoverflow-survey-456106")

    storage_client = storage.Client()
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

                # Pick the largest CSV
                largest_csv = max(csv_files, key=os.path.getsize)
                cleaned_csv_path = os.path.join(
                    temp_dir, f"{year}_cleaned.csv")

                # Copy the largest CSV to the cleaned path
                shutil.copy(largest_csv, cleaned_csv_path)

                # Upload cleaned file to GCS
                destination_blob = f"cleaned_csv/{year}-survey.csv"
                upload_blob = storage_client.bucket(
                    bucket_name).blob(destination_blob)
                upload_blob.upload_from_filename(cleaned_csv_path)

                print(f"Uploaded to GCS: gs://{bucket_name}/{destination_blob}")
