# terraform/variables.tf

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "GCS Bucket name"
  type        = string
}

variable "pipeline_sa_email" {
  description = "Service Account email for pipeline"
  type        = string
}