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

resource "aws_spot_instance_request" "cluster" {
  count = var.instance_count

  ami = "ami-01cc0c9e866d3322b" # raysort-spark-worker-20220328
  #  ami           = "ami-077156789b614cd3c" # samyu's copy of the image
  instance_type        = var.instance_type
  key_name             = "login-us-west-2"
  spot_type            = "one-time"
  wait_for_fulfillment = true
  tags = {
    ClusterName = var.cluster_name
    Name        = "${var.cluster_name}-${format("%03d", count.index)}"
  }

  timeouts {
    delete = "48h"
  }
}

resource "aws_ec2_tag" "cluster_name_tag" {
  count       = length(aws_spot_instance_request.cluster)
  resource_id = aws_spot_instance_request.cluster[count.index].spot_instance_id
  key         = "ClusterName"
  value       = var.cluster_name
}

resource "aws_ec2_tag" "name_tag" {
  count       = length(aws_spot_instance_request.cluster)
  resource_id = aws_spot_instance_request.cluster[count.index].spot_instance_id
  key         = "Name"
  value       = "${var.cluster_name}-${format("%03d", count.index)}-spot"
}
