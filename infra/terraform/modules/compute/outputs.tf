# =============================================================================
# MARINEFLOW — Compute Module: Outputs
# =============================================================================

output "producer_vm_name" {
  description = "Name of the AIS producer VM"
  value       = google_compute_instance.ais_producer.name
}

output "producer_vm_external_ip" {
  description = "External IP of the AIS producer VM"
  value       = google_compute_instance.ais_producer.network_interface[0].access_config[0].nat_ip
}

output "producer_vm_zone" {
  description = "Zone where the VM runs"
  value       = google_compute_instance.ais_producer.zone
}

output "ssh_command" {
  description = "IAP SSH command to connect to the VM"
  value       = "gcloud compute ssh marineflow-ais-producer --zone=${var.zone} --project=${var.project_id} --tunnel-through-iap"
}

output "service_logs_command" {
  description = "Command to tail producer logs on the VM"
  value       = "gcloud compute ssh marineflow-ais-producer --zone=${var.zone} --project=${var.project_id} --tunnel-through-iap --command='sudo journalctl -u ais-producer -f'"
}
