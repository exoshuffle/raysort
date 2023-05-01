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
  default     = "n2-highmem-8"
}

variable "instance_cpu_platform" {
  type        = string
  description = "cpu type for instance"
  default     = "Intel Ice Lake"
}

variable "instance_disk_gb" {
  type        = number
  description = "size in GB for the root disk"
  default     = 40
}

variable "instace_local_disk_count" {
  type        = number
  description = "the number of local disks in each instance"
  default     = 4
}