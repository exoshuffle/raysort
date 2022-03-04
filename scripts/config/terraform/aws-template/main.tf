terraform {
  required_version = ">= 1.1"
  required_providers {
    azurerm = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"
}

resource "aws_instance" "cluster" {
  count = 8

  ami           = "ami-0da5da6db44aaf267" # raysort-hadoop-spark-conda
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
    ClusterName = var.cluster_name
    Name        = "${var.cluster_name}-${format("%03d", count.index)}"
  }
}
