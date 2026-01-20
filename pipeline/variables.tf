variable "credentials" {
    description = "Path to the GCP credentials JSON file"
    default     = "./keys/de-zoomcamp-key.json"
  
}

variable "project" {
    description = "The GCP project ID"
    default     = "de-zoomcamp-484617"
  
}

variable "region" {
    description = "The GCP region"
    default     = "europe-north2"
}

variable "location" {
  description = "The location for GCP resources"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "Name of the BigQuery dataset"
  default     = "de_dataset"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket"
  default     = "de-zoomcamp-484617-terraform-bucket"
}

variable "gcs_storage_class" {
  description = "The Storage Class of the GCS bucket"
  default     = "STANDARD"

}
