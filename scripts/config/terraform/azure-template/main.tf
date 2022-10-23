# Configure the Azure provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  subscription_id = "aa86df77-e703-453e-b2f4-955c3b33e534"
  features {}
}

data "azurerm_resource_group" "app" {
  name = "raysort-eastus"
}

data "azurerm_shared_image" "image" {
  name                = "raysort-2022"
  gallery_name        = "raysortgallery"
  resource_group_name = "lsf-aa"
}

data "azurerm_virtual_network" "app" {
  name                = "lsf-vnet"
  resource_group_name = data.azurerm_resource_group.app.name
}

data "azurerm_subnet" "app" {
  name                 = "default"
  resource_group_name  = data.azurerm_resource_group.app.name
  virtual_network_name = data.azurerm_virtual_network.app.name
}

resource "azurerm_linux_virtual_machine_scale_set" "app" {
  name                = var.cluster_name
  resource_group_name = data.azurerm_resource_group.app.name
  location            = data.azurerm_resource_group.app.location
  sku                 = var.instance_type
  instances           = var.instance_count
  admin_username      = "azureuser"
  source_image_id     = data.azurerm_shared_image.image.id

  admin_ssh_key {
    public_key = file("~/.ssh/authorized_keys")
    username   = "azureuser"
  }

  os_disk {
    storage_account_type = "Premium_LRS"
    caching              = "ReadWrite"
  }

  network_interface {
    name    = "primary"
    primary = true

    ip_configuration {
      name      = "primary"
      primary   = true
      subnet_id = data.azurerm_subnet.app.id
    }
  }
}

data "azurerm_virtual_machine_scale_set" "app" {
  name                = azurerm_linux_virtual_machine_scale_set.app.name
  resource_group_name = data.azurerm_resource_group.app.name
}
