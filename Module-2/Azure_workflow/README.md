# Azure Data Pipeline with Kestra, Terraform, and Synapse

This project automates data ingestion from NYC Taxi data into Azure Synapse Analytics using Kestra workflows, Terraform for infrastructure provisioning, and custom Docker images for dependency management.

## Table of Contents
- [Azure Data Pipeline with Kestra, Terraform, and Synapse](#azure-data-pipeline-with-kestra-terraform-and-synapse)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [File Structure](#file-structure)
  - [Detailed File Breakdown](#detailed-file-breakdown)
    - [flow.yml (Kestra Workflow)](#flowyml-kestra-workflow)
  - [Prerequisites](#prerequisites)
  - [Troubleshooting Guide](#troubleshooting-guide)
    - [1. Connection Failures to Azure Services](#1-connection-failures-to-azure-services)
    - [2. Docker Container Dependency Issues](#2-docker-container-dependency-issues)
    - [3. SQL Schema Mismatch Errors:](#3-sql-schema-mismatch-errors)

---
## Overview
A pipeline that:  
    1. Provisions Azure resources (ADLS Gen2/Synapse) via Terraform   
    2. Executes Kestra workflows to:  
        - Download taxi data   
        - Upload to ADLS Gen2  
        - Create/maintain Synapse tables  
        - Perform UPSERT operations  
    3. Uses a custom Docker image with pre-installed Azure/PyODBC dependencies


## File Structure
|── flow.yaml  
|── main.tf                                              
|── test.ipynb   
|── Dockerfile



## Detailed File Breakdown

### flow.yml (Kestra Workflow)

- Extract the csv file from github api:
    ```yaml
    - id: Extract
        type: io.kestra.plugin.scripts.shell.Commands
        outputFiles:
        - "*.csv"    
        taskRunner:
        type: io.kestra.plugin.core.runner.Process
        commands: 
        - wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{{inputs.taxi_type}}/{{render(vars.file)}}.gz | gunzip > {{render(vars.file)}}
        disabled: true
    ```

- upload the downloaded CSV to ADLSg2
  ```yaml 
    - id: Upload_to_ADLSg2
    type: io.kestra.plugin.azure.storage.adls.Upload
    endpoint: "https://kestradatalake.dfs.core.windows.net"
    connectionString: "{{kv('ADLSg2_CONNECTION_STRING')}}"
    filePath: "{{inputs.taxi_type}}_taxi/{{render(vars.file)}}"
    fileSystem: taxidata
    from: "{{render(vars.data)}}"
    disabled: true
    ```
- Conditional Table Creation:
  ```yaml
    - id: if_green
      type: io.kestra.plugin.core.flow.If
      condition: "{{inputs.taxi_type == 'green'}}"
      then:
        - create_main_green_table: Creates main table schema for green taxis
        - create_external_green_table: Creates external table pointing to ADLS
        - merge_into_main_table: Performs MERGE operation from external to main table

    - id: if_yellow          # Parallel structure for yellow taxis
      condition: "{{inputs.taxi_type == 'yellow'}}"
    ```

### main.tf (Terraform Infrastructure)
- Terraform is used to create all the needed cloud resources.
- Replace all the variables with ones of your own.
- **Core Resources:**
  ```yaml
  # Azure Provider Configuration
  provider "azurerm" {
    features {
      resource_group {
        prevent_deletion_if_contains_resources = false
      }
    }
  }

  # Resource Group
  resource "azurerm_resource_group" "rg" {
    name     = var.resource_group_name
    location = var.location
  }

  # ADLS Gen2 Storage
  resource "azurerm_storage_account" "adls" {
    name                     = var.storage_account_name
    account_kind             = "StorageV2"
    is_hns_enabled           = true  # Required for Gen2
  }

  # Synapse Workspace
  resource "azurerm_synapse_workspace" "synapse" {
    sql_administrator_login              = var.sql_admin_login
    sql_administrator_login_password     = var.sql_admin_password
    storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.adls_fs.id

    identity {
      type = "SystemAssigned"  # Managed Identity
    }
  }
- **Security Configuration:**
  - This firewall is created to give synapse the ability to access adlsg2 account
  - replace the variable with your client ip
  ```yaml
  # Firewall Rule
  resource "azurerm_synapse_firewall_rule" "allow_my_ip" {
    start_ip_address = var.client_ip_address
    end_ip_address   = var.client_ip_address
  }
    ```
### test.ipynb (Validation Scripts)
- this ipynb file is used to check connectivity to different Azure resources before adding them to the flow.
    - ADLS Gen2 Connection Test
    - Synapse Table Operations
    -  Data Merge Operation

### Dockerfile (Custom Environment)
- this image is created to make your workflow environment consistent by forcing it use the same python image across all the different scripts.


## Deployment Steps
1. **Infrastructure Provisioning**
``` bash
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```
2. Run the docker-compose:
  ```bash
    docker compose up -d 
  ```
3. Build Docker Image
```bash 
docker build -t custom-kestra-image:latest .
docker login
docker push custom-kestra-image:latest
```
4. Configure Kestra Secrets for :
- ADLSg2_CONNECTION_STRING
- SYNAPSE_SERVER/DATABASE/USERNAME/PASSWORD

5. Create & execute the Kestra flow

## Prerequisites
- Azure Service Principal with:
  - Storage Blob Data Contributor
  - Synapse Administrator
- Terraform v1.1.0+
- Docker Desktop
- Python 3.11+ with Jupyter for testing

## Troubleshooting Guide

### 1. Connection Failures to Azure Services
- **Symptoms**:  
  - ADLS Gen2/Synapse connection timeouts  
  - "Login failed" errors in SQL scripts  

- **Solutions**:  
  - Verify firewall rules allow outbound traffic on ports 1433 (SQL) and 443 (HTTPS)  
  - Check `azurerm_synapse_firewall_rule` in Terraform matches your current IP  
  - Confirm credentials in Kestra secrets match those in Azure:  
    ```bash
    kv get ADLSg2_CONNECTION_STRING  # Validate connection string format
  - make sure that Synapse managed identity is given the storage blob contributor role in the storage account's IAM
    ``` azurecli
      az role assignment create --role "Storage Blob Data Contributor" \
      --assignee $(az synapse workspace show --name <workspace> --query identity.principalId -o tsv)

### 2. Docker Container Dependency Issues
- **Symptoms**:
  - pyodbc.InterfaceError during SQL operations
  - Missing ODBC driver errors
- **Solutions**:
  1. Rebuild image with debug flags:
    ```Dockerfile
    RUN apt-get install -y --no-install-recommends -V  # Verbose package install
  2. Test ODBC connectivity inside container:
    ```bash
      docker run -it custom-kestra-image /bin/bash
      sqlcmd -S your-synapse-server -U your-user -P your-password

### 3. SQL Schema Mismatch Errors:
- Symptoms:
  - Invalid column name during MERGE operations
  - External table creation failures
- **Solutions**:
  - Compare CSV headers with table schemas in create_main_*_table tasks
  - Validate date formats match between:
    - Source CSV files
    - DATETIME2 column definitions

