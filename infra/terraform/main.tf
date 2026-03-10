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

  # Remote backend in GCS — state lives in the cloud from day one
  # The backend bucket must be created manually ONCE before terraform init (see README)
  backend "gcs" {
    bucket = "marineflow-tfstate"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
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
  source     = "./modules/gcs"
  project_id = var.project_id
  region     = var.region
  labels     = local.common_labels

  depends_on = [module.iam]
}

module "pubsub" {
  source     = "./modules/pubsub"
  project_id = var.project_id
  labels     = local.common_labels

  depends_on = [module.iam]
}

module "bigquery" {
  source     = "./modules/bigquery"
  project_id = var.project_id
  region     = var.region
  labels     = local.common_labels

  depends_on = [module.gcs]
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
