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

# Defined separately due to issue in creation 
# https://github.com/hashicorp/terraform-provider-azurerm/issues/12052
resource "azurerm_storage_account_network_rules" "challenge-storage-network-rules" {
  storage_account_id = azurerm_storage_account.challenge-storage.id

    default_action             = "Deny"
    virtual_network_subnet_ids = [azurerm_subnet.challenge-private.id]
}