variable "credentials" {
  description = "The path to the GCP service account credentials JSON file"
  default     = "../keys/my-creds.json"
}

variable "project" {
  description = "The GCP project ID"
  type        = string
  default     = "bikeshare-analytics-pipeline"
}

variable "location" {
  description = "The location for GCP resources"
  type        = string
  default     = "US"
}

variable "bq_dataset_name" {
  description = "The name of the BigQuery dataset"
  type        = string
  default     = "raw_bikeshare_data"
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket"
  type        =  string
  default     = "bikeshare_data_lake"
}