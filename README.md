
# Stack Overflow End-to-End Data Pipeline

This project demonstrates an end-to-end data pipeline for processing and analyzing the Stack Overflow Developer Survey data from 2011 to 2024. It uses modern data engineering tools and practices including:

- **Terraform** for infrastructure provisioning on GCP
- **Apache Airflow** for orchestration
- **Google Cloud Storage (GCS)** as the data lake
- **Google BigQuery** as the data warehouse
- **PySpark** for data transformation and cleaning
- **Docker** for containerization

---

## 🚀 Pipeline Overview

1. **Data Ingestion**  
   Download raw survey data (CSV/ZIP) and upload to GCS.

2. **Data Processing**  
   Extract and transform the raw files using PySpark.

3. **Data Loading**  
   Load the transformed data into BigQuery.

4. **Orchestration**  
   All steps are managed using Apache Airflow with tasks defined in DAGs.

5. **Infrastructure**  
   Provision GCS buckets and BigQuery datasets/tables using Terraform.

---

## 🗂 Project Structure

```
.
├── dags/                           # Airflow DAGs
│   ├── exampledag.py
│   └── web_to_gs_pipeline.py       # Main pipeline DAG
├── include/                        # Scripts and data for pipeline
│   ├── data.csv
│   ├── scraper.py                  # Web scraping script (if applicable)
│   ├── read.py
│   ├── final_transformation.py     # Final PySpark transformation
│   ├── gcs_extract_upload.py       # Upload raw data to GCS
│   ├── pulling_bigquery_transform.py # Load and transform to BQ
│   └── transform_and_upload_bigquery.py
├── plugins/                        # Airflow plugins (if any)
├── terraform/                      # Infrastructure as code
│   ├── main.tf
│   ├── variables.tf
│   ├── terraform.tfvars
│   └── ...
├── transforms/                     # Additional transformation scripts
├── Dockerfile                      # Custom Dockerfile for Airflow
├── docker-compose.override.yml     # Docker Compose override
├── airflow_settings.yaml           # Airflow metadata DB config
├── my-credentials.json             # GCP service account key (DO NOT COMMIT)
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

---

## 🛠️ Technologies Used

- **Python**: Core language for scripting and ETL tasks
- **Docker**: Containerize the Airflow setup and PySpark transformations
- **Apache Airflow**: Orchestrate ETL pipelines
- **Terraform**: Create GCS buckets and BigQuery datasets
- **Google Cloud Storage**: Store raw and processed data
- **Google BigQuery**: Store final analytics-ready tables
- **PySpark**: Transform inconsistent schemas across years
- **Pandas**: For lightweight processing (if used)

---

## ⚙️ Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/stackoverflow-pipeline.git
cd stackoverflow-pipeline
```

### 2. Create `.env` File

```env
GOOGLE_APPLICATION_CREDENTIALS=./my-credentials.json
PROJECT_ID=your-gcp-project-id
GCS_BUCKET=your-gcs-bucket-name
BQ_DATASET=your-bigquery-dataset
```

### 3. Provision Infrastructure with Terraform

```bash
cd terraform
terraform init
terraform apply
```

### 4. Start Airflow with Docker Compose

```bash
docker-compose up airflow-init
docker-compose up
```

### 5. Access Airflow

Visit `http://localhost:8080` and use:

- **Username**: `admin`
- **Password**: `admin` (default)

Trigger the `web_to_gs_pipeline` DAG to run the pipeline.

---

## 🔄 DAG Workflow

- `web_to_gs_pipeline.py`:
  - Download or scrape data
  - Upload to GCS
  - Transform using PySpark
  - Load clean data to BigQuery

---

## 🔐 Security

- Add `my-credentials.json` to your `.gitignore`
- Do not commit any sensitive information

---

## 📈 Future Enhancements

- Add dbt models for additional BigQuery modeling
- Integrate Data Quality checks (e.g., Great Expectations)
- Schedule daily or weekly automatic runs

---

## 📄 License

MIT License

---

## 🙌 Acknowledgements

Inspired by the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).