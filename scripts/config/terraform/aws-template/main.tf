terraform {
  required_version = ">= 1.1"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"
}

resource "aws_instance" "cluster" {
  count = var.instance_count

  ami           = "ami-03c25329dd95be4f9" # raysort-worker-20220918
  instance_type = var.instance_type
  key_name      = "login-us-west-2"

  #  ebs_block_device {
  #    device_name = "sdb"
  #    iops        = 3000
  #    throughput  = 300
  #    volume_size = 1000
  #    volume_type = "gp3"
  #  }

  tags = {
    ClusterName = var.cluster_name
    Name        = "${var.cluster_name}-${format("%03d", count.index)}"
  }
}
