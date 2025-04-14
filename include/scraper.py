import os
import re
import requests
from bs4 import BeautifulSoup
from google.cloud import storage
from urllib.parse import urljoin, urlparse


def scrape_and_upload():
    BASE_URL = 'https://survey.stackoverflow.co/'
    bucket_name = os.getenv("GCS_BUCKET", "gsbucket-stackoverflow-survey-456106")
    response = requests.get(BASE_URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    search_words = ['Download Full Data Set (CSV)']

    print(f'Scraping base URL: {BASE_URL}')
    print(f'HTTP Status Code: {response.status_code}')

    survey_links = {}

    for link in soup.find_all('a', href=True):
        if any(word in link.text for word in search_words):
            href = link['href']
            full_url = urljoin(BASE_URL, href) if href.startswith('./') else href

            year_match = re.search(r'(20[0-9]{2})', href)
            year = year_match.group(1) if year_match else 'unknown'

            survey_links[year] = full_url
            print(f'Found {year} survey: {full_url}')
            download_and_upload_to_gcs(full_url, bucket_name)

    return survey_links


def download_and_upload_to_gcs(url, bucket_name, destination_blob_name=None):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        if destination_blob_name is None:
            filename = os.path.basename(urlparse(url).path)
            destination_blob_name = f"raw/{filename}"
        elif not destination_blob_name.startswith("raw/"):
            destination_blob_name = f"raw/{destination_blob_name}"

        print(f"Downloading {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        blob = bucket.blob(destination_blob_name)
        print(f"Uploading to gs://{bucket_name}/{destination_blob_name}...")

        # Alternative upload method if open() is not supported in your version
        try:
            with blob.open("wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        except AttributeError:
            # Fallback for older google-cloud-storage versions
            from tempfile import NamedTemporaryFile
            with NamedTemporaryFile() as temp_file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        temp_file.write(chunk)
                temp_file.flush()
                blob.upload_from_filename(temp_file.name)

        print("Upload complete!")
        return True
    except Exception as e:
        print(f"Error in download_and_upload_to_gcs: {str(e)}")
        raise
