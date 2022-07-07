variable "bucket_prefix" {
  type        = string
  description = "a unique name that identifies the cluster and its instances"
}

variable "bucket_count" {
  type        = number
  description = "the number of instances to launch"
  default     = 1
}
