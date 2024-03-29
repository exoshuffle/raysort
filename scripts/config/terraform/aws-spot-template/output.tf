output "instance_ips" {
  description = "Private IP addresses of the EC2 instances"
  value       = aws_spot_instance_request.cluster[*].private_ip
}

output "instance_ids" {
  description = "IDs of the EC2 instances"
  value       = aws_spot_instance_request.cluster[*].id
}
