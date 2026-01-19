terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = "keys/de-zoomcamp-key.json"
  project     = "de-zoomcamp-484617"
  region      = "europe-north2"
}

resource "google_storage_bucket" "terraform_bucket" {
  name          = "de-zoomcamp-484617-terraform-bucket"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
