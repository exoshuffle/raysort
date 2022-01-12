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

provider "aws" {
  region = "us-west-2"
}

resource "aws_instance" "raysort_worker" {
  count = 8

  ami           = "ami-02ccbf3107d9174a7" # ubuntu2004-gcc-conda-py39-raysort
  instance_type = "m6i.4xlarge"
  key_name      = "login-us-west-2"

  ebs_block_device {
    device_name = "sdb"
    iops        = 3000
    throughput  = 500
    volume_size = 1000
    volume_type = "gp3"
  }

  tags = {
    Name = "raysort-worker-${format("%03d", count.index)}"
  }
}


resource "aws_instance" "spark_worker" {
  count = 8

  ami           = "ami-0ed1b8333dc630af0" # raysort-spark-yarn
  instance_type = "m6i.4xlarge"
  key_name      = "login-us-west-2"

  ebs_block_device {
    device_name = "sdb"
    iops        = 3000
    throughput  = 500
    volume_size = 1000
    volume_type = "gp3"
  }

  tags = {
    Name = "spark-worker-${format("%03d", count.index)}"
  }
}
