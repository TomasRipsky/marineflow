# =============================================================================
# MARINEFLOW — Terraform Root
# infra/terraform/main.tf
# =============================================================================

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Remote backend — Terraform state stored in GCS
  # This bucket was created manually before terraform init (see README)
  backend "gcs" {
    bucket = "marineflow-tfstate"
    prefix = "terraform/state"
  }
}

# =============================================================================
# PROVIDER
# Uses Application Default Credentials (ADC)
# Run: gcloud auth application-default login
# =============================================================================

provider "google" {
  project = var.project_id
  region  = var.region
}

# =============================================================================
# LOCALS
# =============================================================================

locals {
  common_labels = {
    project     = "marineflow"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# =============================================================================
# MODULES
# =============================================================================

module "iam" {
  source                = "./modules/iam"
  project_id            = var.project_id
  service_account_email = var.service_account_email
}

module "gcs" {
  source          = "./modules/gcs"
  project_id      = var.project_id
  region          = var.region
  gcs_bucket_name = var.gcs_bucket_name
  storage_class   = var.gcs_storage_class
  labels          = local.common_labels

  depends_on = [module.iam]
}

module "pubsub" {
  source                 = "./modules/pubsub"
  project_id             = var.project_id
  gcs_bucket             = "${var.gcs_bucket_name}-${var.project_id}"
  message_retention_days = var.pubsub_message_retention_days
  ack_deadline_seconds   = var.pubsub_ack_deadline_seconds
  labels                 = local.common_labels

  depends_on = [module.iam, module.gcs]
}

module "cloud_run" {
  source                = "./modules/cloud_run"
  project_id            = var.project_id
  region                = var.region
  service_account_email = module.iam.service_account_email
  labels                = local.common_labels

  depends_on = [module.iam]
}

module "bigquery" {
  source       = "./modules/bigquery"
  project_id   = var.project_id
  gcs_bucket   = var.gcs_bucket_name
  region       = var.region
  bq_location  = var.bq_location
  labels       = local.common_labels

  depends_on = [module.gcs]
}
