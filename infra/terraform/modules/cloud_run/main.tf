# =============================================================================
# MARINEFLOW — Cloud Run Module
# infra/terraform/modules/cloud_run/main.tf
#
# Deploys the AIS Producer as a Cloud Run Job.
#
# Why Cloud Run Job vs Cloud Run Service:
#   - Service: for HTTP request/response workloads (our FastAPI in Phase 5)
#   - Job: for long-running processes with restart semantics (our producer)
#
# The producer connects to aisstream.io via WebSocket and runs indefinitely.
# Cloud Run Jobs with max_retries handles automatic restart on failure —
# equivalent to Docker's restart: unless-stopped but fully managed by GCP.
#
# Authentication:
#   Cloud Run uses the service account's ADC automatically — no credential
#   files to mount, no JSON keys. IAM is handled at the resource level.
#
# API Key:
#   The aisstream.io API key is stored in Secret Manager and injected as
#   an environment variable at runtime — never hardcoded or in Terraform state.
# =============================================================================

# -----------------------------------------------------------------------------
# Artifact Registry — store the producer Docker image
# -----------------------------------------------------------------------------
resource "google_artifact_registry_repository" "marineflow" {
  repository_id = "marineflow"
  format        = "DOCKER"
  location      = var.region
  project       = var.project_id
  description   = "MarineFlow container images"

  labels = var.labels
}

# -----------------------------------------------------------------------------
# Secret Manager — aisstream.io API key
# Stored separately from Terraform state for security.
# Create the secret value manually:
#   gcloud secrets create ais-api-key --project=marineflow-489815
#   echo -n "YOUR_API_KEY" | gcloud secrets versions add ais-api-key --data-file=-
# -----------------------------------------------------------------------------
resource "google_secret_manager_secret" "ais_api_key" {
  secret_id = var.ais_api_key_secret
  project   = var.project_id

  replication {
    auto {}
  }

  labels = var.labels
}

# Grant the service account access to read the secret
resource "google_secret_manager_secret_iam_member" "producer_secret_access" {
  secret_id = google_secret_manager_secret.ais_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.service_account_email}"
}

# -----------------------------------------------------------------------------
# Cloud Run Job — AIS Producer
#
# max_retries = 0 on the Job level means the Job itself doesn't retry.
# The container's restart policy handles reconnection internally via tenacity.
# If the container exits (crash, not reconnection failure), Cloud Run
# restarts it automatically via the execution's restart policy.
# -----------------------------------------------------------------------------
resource "google_cloud_run_v2_job" "ais_producer" {
  name     = "ais-producer"
  location = var.region
  project  = var.project_id

  labels = var.labels

  template {
    template {
      service_account = var.service_account_email

      # Restart the container if it exits — equivalent to restart: unless-stopped
      max_retries = 10

      timeout = "86400s"   # 24h max execution time per attempt

      containers {
        image = var.producer_image

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }

        # Environment variables — non-sensitive
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }
        env {
          name  = "PUBSUB_TOPIC_POSITIONS"
          value = var.pubsub_topic_positions
        }
        env {
          name  = "PUBSUB_TOPIC_METADATA"
          value = var.pubsub_topic_metadata
        }
        env {
          name  = "PUBSUB_TOPIC_DLQ"
          value = var.pubsub_topic_dlq
        }
        env {
          name  = "MESSAGE_SOURCE"
          value = "aisstream_live"
        }
        env {
          name  = "LOG_FORMAT"
          value = "json"   # structured logs for Cloud Logging
        }

        # API key injected from Secret Manager at runtime
        env {
          name = "AIS_API_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.ais_api_key.secret_id
              version = "latest"
            }
          }
        }
      }
    }
  }

  depends_on = [
    google_secret_manager_secret_iam_member.producer_secret_access,
  ]
}
