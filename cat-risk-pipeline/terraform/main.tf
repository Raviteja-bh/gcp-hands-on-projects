# terraform/main.tf

terraform {
  backend "gcs" {
    bucket = "cat-risk-pipeline-bucket-2026"
    prefix = "terraform/state"
  }
}

#provider
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}


#enable API's
resource "google_project_service" "apis" {
  for_each = toset([
    "cloudfunctions.googleapis.com",
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "bigquery.googleapis.com",
    "cloudscheduler.googleapis.com",
    "composer.googleapis.com",
    "storage.googleapis.com"
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}


#cloud storage bucket
resource "google_storage_bucket" "pipeline_bucket" {
  name                        = var.bucket_name
  location                    = "US"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true # Fix for your Org Policy error
  force_destroy               = true

  # Auto delete files after 90 days
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  # Versioning — keeps old versions of files
  versioning {
    enabled = true
  }
}


# Create folders inside bucket
resource "google_storage_bucket_object" "folders" {
  for_each = toset([
    "dataflow-staging/",
    "dataflow-temp/",
    "raw-data/",
    "pipeline/"
  ])

  name    = each.value
  content = " "
  bucket  = google_storage_bucket.pipeline_bucket.name
}


#pub/sub
resource "google_pubsub_topic" "cat_risk_topic" {
  name    = "cat-risk-raw-events"
  project = var.project_id

  # Messages expire after 7 days
  message_retention_duration = "604800s"
}

resource "google_pubsub_subscription" "cat_risk_sub" {
  name    = "cat-risk-sub"
  topic   = google_pubsub_topic.cat_risk_topic.name
  project = var.project_id

  # 60 seconds to ACK before retry
  ack_deadline_seconds = 60

  # Keep unacked messages for 7 days
  message_retention_duration = "604800s"

  # Delete subscription after 31 days of inactivity
  expiration_policy {
    ttl = "2678400s"
  }
}


#BigQuery
resource "google_bigquery_dataset" "cat_risk" {
  dataset_id    = "cat_risk"
  friendly_name = "Catastrophe Risk Dataset"
  description   = "Real-time catastrophe event data"
  location      = "US"
  project       = var.project_id
}

resource "google_bigquery_table" "events" {
  dataset_id = google_bigquery_dataset.cat_risk.dataset_id
  table_id   = "events"
  project    = var.project_id

  # Partition by day on timestamp field
  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  # Cluster by source, event_type, severity
  clustering = ["source", "event_type", "severity"]

  # Schema
  schema = jsonencode([
    { name = "source",           type = "STRING",    mode = "REQUIRED" },
    { name = "event_type",       type = "STRING",    mode = "REQUIRED" },
    { name = "title",            type = "STRING",    mode = "NULLABLE" },
    { name = "magnitude",        type = "FLOAT",     mode = "NULLABLE" },
    { name = "location",         type = "STRING",    mode = "REQUIRED" },
    { name = "latitude",         type = "FLOAT",     mode = "NULLABLE" },
    { name = "longitude",        type = "FLOAT",     mode = "NULLABLE" },
    { name = "severity",         type = "STRING",    mode = "NULLABLE" },
    { name = "risk_score",       type = "FLOAT",     mode = "NULLABLE" },
    { name = "timestamp",        type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "processed_at",     type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "pipeline_version", type = "STRING",    mode = "NULLABLE" }
  ])
}


# Service Accounts + IAM roles
resource "google_service_account" "pipeline_sa" {
  account_id   = "pipeline-sa"
  display_name = "Cat Risk Pipeline Service Account"
  project      = var.project_id
}

resource "google_project_iam_member" "pipeline_roles" {
  for_each = toset([
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/dataflow.worker",
    "roles/bigquery.dataEditor",
    "roles/pubsub.viewer",
    "roles/storage.objectAdmin"
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
# This allows the Service Account to use itself, and allows 
# the Airflow/Composer identity to trigger jobs using this SA.
resource "google_service_account_iam_member" "sa_user_binding" {
  service_account_id = google_service_account.pipeline_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.pipeline_sa.email}"
}


#cloud scheduler
resource "google_cloud_scheduler_job" "pipeline_trigger" {
  name      = "fetch-cat-events-scheduler"
  schedule  = "*/15 * * * *"
  time_zone = "UTC"
  region    = var.region
  project   = var.project_id

  http_target {
    uri         = "https://${var.region}-${var.project_id}.cloudfunctions.net/fetch-cat-events"
    http_method = "POST"
    body        = base64encode("{}")

    oidc_token {
      service_account_email = google_service_account.pipeline_sa.email
    }
  }

  retry_config {
    retry_count = 3
  }
}

resource "google_dataflow_job" "pubsub_to_bq_job" {
  name              = "cat-risk-dataflow-stream"
  template_gcs_path = "gs://dataflow-templates-us-central1/latest/PubSub_to_BigQuery"
  region            = "us-central1"
  
  # Specify the correct service account for the workers
  service_account_email = google_service_account.pipeline_sa.email

  parameters = {
    inputTopic      = google_pubsub_topic.cat_risk_topic.id
    outputTableSpec = "${var.project_id}:cat_risk.events"
  }

  temp_gcs_location = "gs://${google_storage_bucket.pipeline_bucket.name}/dataflow-temp"
  on_delete         = "cancel"
}