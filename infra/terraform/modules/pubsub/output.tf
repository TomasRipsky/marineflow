# =============================================================================
# MARINEFLOW — Pub/Sub Module: Outputs
# infra/terraform/modules/pubsub/outputs.tf
# =============================================================================

output "topic_ids" {
  description = "Map of all topic names to their full resource IDs"
  value       = { for k, v in google_pubsub_topic.topics : k => v.id }
}

output "topic_names" {
  description = "Map of all topic names to their short names"
  value       = { for k, v in google_pubsub_topic.topics : k => v.name }
}

output "subscription_ids" {
  description = "Map of all Spark subscription names to their full resource IDs"
  value       = { for k, v in google_pubsub_subscription.spark_subscriptions : k => v.id }
}

output "vessel_positions_topic" {
  description = "Name of the vessel-positions topic (used by the AIS producer)"
  value       = google_pubsub_topic.topics["vessel-positions"].name
}

output "vessel_positions_subscription" {
  description = "Name of the vessel-positions Spark subscription (used by Spark Streaming)"
  value       = google_pubsub_subscription.spark_subscriptions["vessel-positions"].name
}

output "vessel_metadata_topic" {
  description = "Name of the vessel-metadata topic"
  value       = google_pubsub_topic.topics["vessel-metadata"].name
}

output "port_events_topic" {
  description = "Name of the port-events topic"
  value       = google_pubsub_topic.topics["port-events"].name
}

output "alerts_topic" {
  description = "Name of the maritime-alerts topic (used by the Anomaly Detector)"
  value       = google_pubsub_topic.topics["maritime-alerts"].name
}

output "alerts_api_subscription" {
  description = "Name of the alerts subscription consumed by FastAPI"
  value       = google_pubsub_subscription.api_alerts_subscription.name
}

output "dlq_topic" {
  description = "Name of the dead-letter-queue topic"
  value       = google_pubsub_topic.topics["dead-letter-queue"].name
}
