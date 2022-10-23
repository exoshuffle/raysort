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
  name = "raysort-westus"
}

resource "azurerm_storage_account" "app" {
  count                    = var.account_count
  name                     = "${var.account_prefix}${data.azurerm_resource_group.app.location}${format("%03d", count.index)}"
  resource_group_name      = data.azurerm_resource_group.app.name
  location                 = data.azurerm_resource_group.app.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

data "azurerm_storage_account_sas" "app" {
  count             = var.account_count
  connection_string = azurerm_storage_account.app[count.index].primary_connection_string

  resource_types {
    service   = true
    container = false
    object    = false
  }

  services {
    blob  = true
    queue = false
    table = false
    file  = false
  }

  start  = timestamp()
  expiry = "2023-01-01T00:00:00Z"

  permissions {
    read    = true
    write   = true
    delete  = true
    list    = true
    add     = false
    create  = false
    update  = false
    process = false
    tag     = false
    filter  = false
  }
}

resource "azurerm_storage_container" "app" {
  count                = var.account_count * var.container_count_per_account
  name                 = "${format("%03d", count.index % var.container_count_per_account)}-${var.container_prefix}"
  storage_account_name = azurerm_storage_account.app[floor(count.index / var.container_count_per_account)].name
}
