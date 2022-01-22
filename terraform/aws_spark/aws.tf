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

variable "num_workers" {
  type        = number
  description = "Number of workers to spawn"
}

provider "aws" {
  region = "us-west-2"
}

resource "aws_instance" "raysort_spark_worker" {
  count = var.num_workers

  ami           = "ami-04760daecb68807d3" # raysort-spark-worker
  instance_type = "r6i.2xlarge"
  key_name      = "login-us-west-2"

  ebs_block_device {
    device_name = "sdb"
    iops        = 3000
    throughput  = 300
    volume_size = 1000
    volume_type = "gp3"
  }

  tags = {
    Name = "raysort-spark-worker-${format("${var.num_workers}-%02d", count.index)}"
  }
}
