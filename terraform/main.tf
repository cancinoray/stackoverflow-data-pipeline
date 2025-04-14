terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.28.0"
    }
  }
}

provider "google" {
  project     = var.project
  region      = var.region
  credentials = file(var.credentials_file_path)
}

resource "google_storage_bucket" "data-lake-bucket" {
  name     = var.gcs_bucket_name
  location = var.location

  # Optional, but recommended settings:
  storage_class               = var.gcs_storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 // days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "data-warehouse" {
  dataset_id = var.bq_dataset_name
  project    = var.project
  location   = var.location
}
