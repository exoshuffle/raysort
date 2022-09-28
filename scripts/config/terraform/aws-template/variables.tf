variable "cluster_name" {
  type        = string
  description = "a unique name that identifies the cluster and its instances"
}

variable "instance_count" {
  type        = number
  description = "the number of instances to launch"
  default     = 1
}

variable "instance_type" {
  type        = string
  description = "the type of instances to launch"
  default     = "r6i.2xlarge"
}

variable "instance_disk_gb" {
  type        = number
  description = "size in GB for the root disk"
  default     = 40
}
