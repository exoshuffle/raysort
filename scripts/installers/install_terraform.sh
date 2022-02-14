#!/bin/bash

set -ex

# Reference: https://learn.hashicorp.com/tutorials/terraform/install-cli

# Install Terraform

sudo apt-get update && sudo apt-get install -y gnupg software-properties-common curl
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
sudo apt-get update && sudo apt-get install terraform

# Verify the installation

terraform -version

# Create cache directory

mkdir -p ~/.terraform.d/plugin-cache
