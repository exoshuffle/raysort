output "sas" {
  value     = data.azurerm_storage_account_sas.app.*.sas
  sensitive = true
}
