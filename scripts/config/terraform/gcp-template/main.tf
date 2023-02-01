terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

provider "google" {
  credentials = file("gcp-terraform-credentials.json")

  project = "exoshuffle"
  region  = "us-west1"
  zone    = "us-west1-b"
}

data "google_compute_network" "vpc_network" {
  name = "exoshuffle-vpc"
}

resource "google_compute_instance" "vm_instance" {
    count = var.instance_count

    name = "${var.cluster_name}-${format("%03d", count.index)}"
    machine_type = var.instance_type
    tags = [ var.cluster_name ]
    zone = "us-west1-b"
    allow_stopping_for_update = true
    min_cpu_platform = var.instance_cpu_platform

    boot_disk {
        initialize_params {
            image = "debian-cloud/debian-11"
            size = var.instance_disk_gb
            type = "pd-standard"
        }
    }

    dynamic "scratch_disk" {
        for_each = range(var.instace_local_disk_count)
        content {
            interface = "NVME"
        }
    }

    network_interface {
        network = data.google_compute_network.vpc_network.name
    }
}
