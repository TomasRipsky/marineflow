# =============================================================================
# MARINEFLOW — IAM Module: Variables
# infra/terraform/modules/iam/variables.tf
# =============================================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "service_account_email" {
  description = "Email of the Service Account to bind roles to"
  type        = string
}
