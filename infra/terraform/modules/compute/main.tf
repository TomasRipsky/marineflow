# =============================================================================
# MARINEFLOW — Compute Module
# infra/terraform/modules/compute/main.tf
#
# Deploys an e2-micro VM to run the AIS Producer as a persistent process.
#
# Why a VM instead of Cloud Run / Cloud Functions:
#   The AIS producer maintains a long-lived WebSocket connection to aisstream.io.
#   Cloud Run Jobs and Cloud Functions have TCP idle timeouts (~10 min) that
#   kill WebSocket connections even when ping_interval is configured.
#   A VM has no such timeouts — the process runs indefinitely until stopped.
#
# Cost: e2-micro is eligible for the GCP free tier (1 instance/month in us-*).
#
# Authentication:
#   The VM uses the project service account via ADC — no credential files needed.
#   The startup script fetches the API key from Secret Manager at boot time.
#
# Process management:
#   The producer runs as a systemd service — auto-starts on boot, restarts
#   on failure with exponential backoff.
# =============================================================================

# -----------------------------------------------------------------------------
# Secret Manager — aisstream.io API key
# The secret resource is created here; the value is added manually:
#   echo -n "YOUR_KEY" | gcloud secrets versions add ais-api-key --data-file=-
# -----------------------------------------------------------------------------
resource "google_secret_manager_secret" "ais_api_key" {
  secret_id = var.ais_api_key_secret
  project   = var.project_id

  replication {
    auto {}
  }

  labels = var.labels
}

resource "google_secret_manager_secret_iam_member" "producer_secret_access" {
  secret_id = google_secret_manager_secret.ais_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.service_account_email}"
}

# -----------------------------------------------------------------------------
# Firewall — allow SSH for maintenance (restricted to IAP tunnel)
# -----------------------------------------------------------------------------
resource "google_compute_firewall" "allow_iap_ssh" {
  name    = "marineflow-allow-iap-ssh"
  network = "default"
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  # Only allow SSH from Google's IAP range — no direct internet SSH exposure
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["marineflow-producer"]

  description = "Allow SSH via IAP tunnel for producer VM maintenance"
}

# -----------------------------------------------------------------------------
# VM — e2-micro (free tier eligible in us-* regions)
# -----------------------------------------------------------------------------
resource "google_compute_instance" "ais_producer" {
  name         = "marineflow-ais-producer"
  machine_type = "e2-micro"
  zone         = var.zone
  project      = var.project_id

  tags   = ["marineflow-producer"]
  labels = merge(var.labels, { role = "ais-producer" })

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 10   # GB — minimal, code + deps fit easily
      type  = "pd-standard"
    }
  }

  network_interface {
    network = "default"
    # No access_config = no external IP — uses Cloud NAT for outbound traffic
    # This is more secure and still allows WebSocket connections outbound
    access_config {}   # ephemeral external IP — simplest for portfolio
  }

  service_account {
    email  = var.service_account_email
    scopes = ["cloud-platform"]   # full GCP access gated by IAM roles
  }

  # Startup script — runs once at first boot
  # Sets up Python environment, clones code, creates systemd service
  metadata = {
    startup-script = <<-SCRIPT
      #!/bin/bash
      set -euo pipefail
      exec > /var/log/marineflow-startup.log 2>&1

      echo "=== MarineFlow AIS Producer startup ==="

      # Install dependencies
      apt-get update -qq
      apt-get install -y -qq python3.11 python3-pip python3-venv git

      # Create app user
      useradd -m -s /bin/bash marineflow || true

      # Clone repo (or pull latest if already cloned)
      REPO_DIR="/home/marineflow/app"
      if [ -d "$REPO_DIR/.git" ]; then
        cd "$REPO_DIR" && git pull
      else
        git clone ${var.repo_url != "" ? var.repo_url : "https://github.com/your-org/marineflow.git"} "$REPO_DIR"
      fi

      # Install Python deps
      cd "$REPO_DIR/ingestion/ais_producer"
      python3 -m venv /home/marineflow/venv
      /home/marineflow/venv/bin/pip install --quiet -r requirements.txt

      # Fetch API key from Secret Manager
      AIS_API_KEY=$(gcloud secrets versions access latest \
        --secret="${var.ais_api_key_secret}" \
        --project="${var.project_id}")

      # Write environment file
      cat > /home/marineflow/producer.env <<EOF
      GCP_PROJECT_ID=${var.project_id}
      GOOGLE_CLOUD_PROJECT=${var.project_id}
      PUBSUB_TOPIC_POSITIONS=${var.pubsub_topic_positions}
      PUBSUB_TOPIC_METADATA=${var.pubsub_topic_metadata}
      PUBSUB_TOPIC_DLQ=${var.pubsub_topic_dlq}
      MESSAGE_SOURCE=aisstream_live
      LOG_FORMAT=json
      AIS_API_KEY=$AIS_API_KEY
      EOF

      chown marineflow:marineflow /home/marineflow/producer.env
      chmod 600 /home/marineflow/producer.env

      # Create systemd service
      cat > /etc/systemd/system/ais-producer.service <<EOF
      [Unit]
      Description=MarineFlow AIS Producer
      After=network-online.target
      Wants=network-online.target

      [Service]
      Type=simple
      User=marineflow
      WorkingDirectory=/home/marineflow/app/ingestion/ais_producer
      EnvironmentFile=/home/marineflow/producer.env
      ExecStart=/home/marineflow/venv/bin/python main.py
      Restart=on-failure
      RestartSec=10
      StartLimitIntervalSec=300
      StartLimitBurst=5

      # Logging to journald (visible via: journalctl -u ais-producer -f)
      StandardOutput=journal
      StandardError=journal
      SyslogIdentifier=ais-producer

      [Install]
      WantedBy=multi-user.target
      EOF

      # Enable and start the service
      systemctl daemon-reload
      systemctl enable ais-producer
      systemctl start ais-producer

      echo "=== Startup complete — ais-producer service started ==="
    SCRIPT
  }

  # Allow Terraform to update metadata without recreating the VM
  lifecycle {
    ignore_changes = [metadata["startup-script"]]
  }
}
