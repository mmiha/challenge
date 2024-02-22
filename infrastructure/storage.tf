resource "azurerm_storage_account" "challenge-storage" {
  name                     = "challengestorageacc"
  resource_group_name      = azurerm_resource_group.challenge.name
  location                 = azurerm_resource_group.challenge.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource azurerm_storage_container "challenge-storage-container" {
  name                  = "challenge-data"
  storage_account_name  = azurerm_storage_account.challenge-storage.name
  container_access_type = "private"
}
