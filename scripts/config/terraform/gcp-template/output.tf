output "instance_ips" {
    description = "Private IP addresses of the instances"
    value = google_compute_instance.vm_instance[*].network_interface.0.network_ip
}
