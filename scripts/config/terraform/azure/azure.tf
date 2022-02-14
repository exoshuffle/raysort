# Configure the Azure provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 2.0"
    }
  }

  required_version = ">= 0.14"
}

provider "azurerm" {
  features {}
}

data "azurerm_shared_image" "image" {
  name                = "raysort-general"
  gallery_name        = "raysortgallery"
  resource_group_name = "lsf-aa"
}

resource "azurerm_resource_group" "app" {
  name     = "raysort-ncus-rg"
  location = "northcentralus"
}

resource "azurerm_virtual_network" "app" {
  name                = "raysort-vnet"
  address_space       = ["10.10.0.0/16"]
  resource_group_name = azurerm_resource_group.app.name
  location            = azurerm_resource_group.app.location
}

resource "azurerm_subnet" "app" {
  name                 = "raysort-subnet"
  resource_group_name  = azurerm_resource_group.app.name
  virtual_network_name = azurerm_virtual_network.app.name
  address_prefixes     = ["10.10.0.0/24"]
}

resource "azurerm_linux_virtual_machine_scale_set" "app" {
  name                = "raysort-ncus"
  resource_group_name = azurerm_resource_group.app.name
  location            = azurerm_resource_group.app.location
  sku                 = "Standard_E8bs_v5"
  instances           = 40
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

  data_disk {
    storage_account_type = "Premium_LRS"
    caching              = "None"
    disk_size_gb         = 8192
    lun                  = 0
  }

  network_interface {
    name    = "primary"
    primary = true

    ip_configuration {
      name      = "primary"
      primary   = true
      subnet_id = azurerm_subnet.app.id
    }
  }
}
