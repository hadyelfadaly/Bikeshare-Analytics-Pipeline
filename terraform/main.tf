terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = "us-central1"
}

resource "google_storage_bucket" "data_lake_bucket" {
  name          = var.gcs_bucket_name 
  location      = var.location
  force_destroy = true
}

resource "google_bigquery_dataset" "raw_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}