resource "azurerm_network_security_group" "challenge-public" {
  name                = "challenge-public-security-group"
  location            = azurerm_resource_group.challenge.location
  resource_group_name = azurerm_resource_group.challenge.name
}

resource "azurerm_network_security_group" "challenge-private" {
  name                = "challenge-private-security-group"
  location            = azurerm_resource_group.challenge.location
  resource_group_name = azurerm_resource_group.challenge.name
}

resource "azurerm_virtual_network" "challenge" {
  name                = "challenge-network"
  location            = azurerm_resource_group.challenge.location
  resource_group_name = azurerm_resource_group.challenge.name
  address_space       = ["10.0.0.0/16"]
  dns_servers         = ["10.0.0.4", "10.0.0.5"]
}

resource "azurerm_subnet" "challenge-public" {
    resource_group_name = azurerm_resource_group.challenge.name
    virtual_network_name = azurerm_virtual_network.challenge.name
    name           = "public-subnet"
    address_prefixes = ["10.0.1.0/24"]

    # https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/databricks_workspace#vnet_address_prefix
    delegation {
        name = "challenge-public-delegation"
        service_delegation {
            name = "Microsoft.Databricks/workspaces"
            # Automatically created otherwise
            actions = [
                "Microsoft.Network/virtualNetworks/subnets/join/action",
                "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
                "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
            ]
        }
    }
}

resource "azurerm_subnet" "challenge-private" {
    resource_group_name = azurerm_resource_group.challenge.name
    virtual_network_name = azurerm_virtual_network.challenge.name
    name           = "private-subnet"
    address_prefixes = ["10.0.2.0/24"]
    service_endpoints = ["Microsoft.Storage"]

    # https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/databricks_workspace#vnet_address_prefix
    delegation {
        name = "challenge-private-delegation"
        service_delegation {
            name = "Microsoft.Databricks/workspaces"

            # Automatically created otherwise
            actions = [
                "Microsoft.Network/virtualNetworks/subnets/join/action",
                "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
                "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
            ]
        }
    }

}

resource "azurerm_subnet_network_security_group_association" "challenge-public" {
  subnet_id                 = azurerm_subnet.challenge-public.id
  network_security_group_id = azurerm_network_security_group.challenge-public.id
}

resource "azurerm_subnet_network_security_group_association" "challenge-private" {
  subnet_id                 = azurerm_subnet.challenge-private.id
  network_security_group_id = azurerm_network_security_group.challenge-private.id
}