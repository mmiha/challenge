resource "azurerm_resource_group" "challenge" {
  name     = "challenge-resources"
  location = "Germany West Central"
}

resource "azurerm_databricks_workspace" "challenge-workspace" {
  name                        = "challenge-workspace"
  resource_group_name         = azurerm_resource_group.challenge.name
  location                    = azurerm_resource_group.challenge.location
  sku                         = "trial"
}

resource "databricks_secret_scope" "challenge-storage-secret-scope" {
  name = "storage-secret-scope"
}

resource "databricks_secret" "publishing_api" {
  key          = "storage_conn_string"
  string_value = azurerm_storage_account.challenge-storage.primary_connection_string
  scope        = databricks_secret_scope.challenge-storage-secret-scope.id
}

resource "databricks_repo" "challenge-repo" {
  url = "https://github.com/mmiha/challenge.git"
}