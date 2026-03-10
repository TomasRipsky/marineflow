# MarineFlow — GCP Infrastructure with Terraform

## First-time setup order

### PRE-REQUISITE: Create the Terraform state bucket manually
This bucket must exist BEFORE running `terraform init` because the backend configuration depends on it.

```bash
# Run only ONCE for the entire lifetime of the project
gcloud storage buckets create gs://marineflow-tfstate \
  --location=us-central1 \
  --uniform-bucket-level-access

# Enable versioning to allow state recovery
gcloud storage buckets update gs://marineflow-tfstate \
  --versioning
```

### 1. Configure variables
```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your real values
```

### 2. Initialize Terraform
```bash
terraform init
```

### 3. Validate configuration
```bash
terraform validate
terraform fmt -recursive
```

### 4. Review the plan before applying
```bash
terraform plan -out=marineflow.tfplan
```

### 5. Apply infrastructure
```bash
terraform apply marineflow.tfplan
```

## Resources created

### GCS Buckets
| Bucket | Purpose |
|--------|---------|
| `marineflow-tfstate` | Terraform remote state (created manually) |
| `marineflow-lake-{project_id}` | Data lake: bronze / silver / gold / checkpoints / models |

### Pub/Sub Topics
| Topic | Description |
|-------|-------------|
| `vessel-positions` | Real-time AIS position stream |
| `vessel-metadata` | Static vessel information |
| `port-events` | Port arrival and departure events |
| `maritime-alerts` | Anomaly Detector output alerts |
| `dead-letter-queue` | Failed messages for diagnosis and replay |

### BigQuery Datasets
| Dataset | Layer |
|---------|-------|
| `marineflow_bronze` | Raw data, no transformations |
| `marineflow_silver` | Cleaned and geospatially enriched data |
| `marineflow_gold` | Analytical tables and dbt model outputs |
| `marineflow_features` | ML Feature Store |

## Useful commands

```bash
# Inspect current state
terraform show

# Display outputs (URLs, resource IDs)
terraform output

# Destroy all resources (use with caution in production)
terraform destroy

# Apply changes to a specific module only
terraform apply -target=module.pubsub
```

## Important notes

- `terraform.tfvars` and `gcp-credentials.json` are listed in `.gitignore` — they must never reach the repository
- The tfstate bucket has versioning enabled — previous states can be recovered if needed
- BigQuery datasets have `delete_contents_on_destroy = true` in dev — set to `false` before promoting to production
