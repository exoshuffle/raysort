# Configure the Azure provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }

  required_version = ">= 0.14"
}

# Use when applying terraform: terraform apply -var="num_workers=8"

variable "num_workers" {
  type        = number
  description = "Number of workers to launch"
  default     = 8
}

provider "aws" {
  region = "us-west-2"
}

resource "aws_instance" "raysort_nvme_worker" {
  count = var.num_workers

  ami           = "ami-0da5da6db44aaf267" # raysort-hadoop-spark-conda
  instance_type = "r6i.2xlarge"
  key_name      = "login-us-west-2"

  ebs_block_device {
    device_name = "sdb"
    volume_size = 4096
    volume_type = "sc1"
  }

  tags = {
    Name = "raysort-hdd-worker-${format("${var.num_workers}-%03d", count.index)}"
  }
}
