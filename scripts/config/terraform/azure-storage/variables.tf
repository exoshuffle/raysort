variable "account_prefix" {
  type        = string
  description = "account prefix"
  default     = "raysort"
}

variable "account_count" {
  type        = number
  description = "the number of accounts to create"
  default     = 2
}

variable "container_prefix" {
  type        = string
  description = "container prefix"
  default     = "raysort"
}

variable "container_count_per_account" {
  type        = number
  description = "the number of containers to create in an account"
  default     = 10
}
