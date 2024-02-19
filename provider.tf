provider "azurerm" {
  features {}
}

provider "databricks" {
    azure_workspace_resource_id = azurerm_databricks_workspace.challenge-workspace.id
    host = azurerm_databricks_workspace.challenge-workspace.workspace_url
}