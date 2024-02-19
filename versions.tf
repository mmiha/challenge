terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      version =  ">= 3.92"
    }
    databricks = {
    source  = "databricks/databricks"
    version = "1.36.3"
    }
  }
}
