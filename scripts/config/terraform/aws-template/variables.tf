variable "cluster_name" {
  type        = string
  description = "a unique name that identifies the cluster and its instances"
}

variable "instance_type" {
  type        = string
  description = "the type of instances to launch"
}
