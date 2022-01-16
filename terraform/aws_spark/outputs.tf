output "instance_private_ip" {
  description = "Private IP addresses of the EC2 instances"
  value       = aws_instance.raysort_spark_worker.*.private_ip
}
