# Configure the Azure provider
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0.2"
    }
  }

  required_version = ">= 1.1.0"
}

# Configure the Azure provider
provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false # Disable the safety check
    }
  }
}

# Create a resource group
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

# Create an Azure Data Lake Storage Gen2 account
resource "azurerm_storage_account" "adls" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Enable Hierarchical Namespace for ADLS Gen2
}

# Create a filesystem (container) in the ADLS Gen2 account
resource "azurerm_storage_data_lake_gen2_filesystem" "adls_fs" {
  name               = var.filesystem_name
  storage_account_id = azurerm_storage_account.adls.id
}

# Create an Azure Synapse Analytics workspace
resource "azurerm_synapse_workspace" "synapse" {
  name                                 = var.synapse_workspace_name
  resource_group_name                  = azurerm_resource_group.rg.name
  location                             = azurerm_resource_group.rg.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.adls_fs.id
  sql_administrator_login              = var.sql_admin_login
  sql_administrator_login_password     = var.sql_admin_password

  identity {
    type = "SystemAssigned"
  }
}

# Create a dedicated SQL pool in Azure Synapse Analytics
resource "azurerm_synapse_sql_pool" "dedicated_sql_pool" {
  name                 = var.sql_pool_name
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  sku_name             = var.sql_pool_sku
  create_mode          = "Default"
}

# Add a firewall rule to allow your client IP address
# before creating this firewall rule, your synapse work space wouldn't be able to connect to the adlsg2 account
resource "azurerm_synapse_firewall_rule" "allow_my_ip" {
  name                 = var.firewall_rule_name
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  start_ip_address     = var.client_ip_address
  end_ip_address       = var.client_ip_address
}

# Output the resource group name, storage account name, and Synapse workspace name
output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "storage_account_name" {
  value = azurerm_storage_account.adls.name
}

output "synapse_workspace_name" {
  value = azurerm_synapse_workspace.synapse.name
}