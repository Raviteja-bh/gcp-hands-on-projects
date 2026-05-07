# terraform/outputs.tf

output "bucket_url" {
  description = "GCS Bucket URL"
  value       = "gs://${google_storage_bucket.pipeline_bucket.name}"
}

output "pubsub_topic" {
  description = "Pub/Sub Topic name"
  value       = google_pubsub_topic.cat_risk_topic.name
}

output "pubsub_subscription" {
  description = "Pub/Sub Subscription name"
  value       = google_pubsub_subscription.cat_risk_sub.name
}

output "bigquery_table" {
  description = "BigQuery table full path"
  value       = "${var.project_id}.${google_bigquery_dataset.cat_risk.dataset_id}.${google_bigquery_table.events.table_id}"
}

output "service_account_email" {
  description = "Pipeline Service Account email"
  value       = google_service_account.pipeline_sa.email
}