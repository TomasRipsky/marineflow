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
  # The bucket must exist before terraform init (see modules/gcs/main.tf and the README)
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

module "bigquery" {
  source       = "./modules/bigquery"
  project_id   = var.project_id
  gcs_bucket   = var.gcs_bucket_name
  region       = var.region
  bq_location  = var.bq_location
  labels       = local.common_labels

  depends_on = [module.gcs]
}
