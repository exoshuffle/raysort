variable "bucket_prefix" {
  type        = string
  description = "bucket prefix"
  default     = "cloudsort"
}

variable "bucket_count" {
  type        = number
  description = "the number of buckets to create"
  default     = 40
}
