output "instance_ips" {
  description = "Private IP addresses of the instances"
  value       = data.azurerm_virtual_machine_scale_set.app.instances.*.private_ip_address
}
