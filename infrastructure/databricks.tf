resource "azurerm_resource_group" "challenge" {
  name     = "challenge-resources"
  location = "Germany West Central"
}

resource "azurerm_databricks_workspace" "challenge-workspace" {
  name                        = "challenge-workspace"
  resource_group_name         = azurerm_resource_group.challenge.name
  location                    = azurerm_resource_group.challenge.location
  sku                         = "trial"
 
  custom_parameters {
    virtual_network_id = azurerm_virtual_network.challenge.id
    public_subnet_name = "public-subnet"
    public_subnet_network_security_group_association_id = azurerm_subnet.challenge-public.id
    private_subnet_name = "private-subnet"
    private_subnet_network_security_group_association_id = azurerm_subnet.challenge-private.id
  }

  depends_on = [
    azurerm_storage_account_network_rules.challenge-storage-network-rules,
    azurerm_storage_container.challenge-storage-container
  ]
}

resource "databricks_secret_scope" "challenge-storage-secret-scope" {
  name = "storage-secret-scope"
  
  depends_on = [
    azurerm_databricks_workspace.challenge-workspace
  ]
}

resource "databricks_secret" "publishing_api" {
  key          = "storage_conn_string"
  string_value = azurerm_storage_account.challenge-storage.primary_connection_string
  scope        = databricks_secret_scope.challenge-storage-secret-scope.id
  
  depends_on = [
    azurerm_databricks_workspace.challenge-workspace
  ]
}


data "databricks_spark_version" "latest" {
  gpu = false
  
  depends_on = [
    azurerm_databricks_workspace.challenge-workspace
  ]
}

resource "databricks_job" "challenge-job" {
 name = "challenge-job"
 max_concurrent_runs = 1

 # job schedule
 schedule {
   quartz_cron_expression = "0 0 12 ? * MON-FRI"
   timezone_id = "UTC"
  }

 # reference to git repo. Add the git credential separately
 # through a databricks_git_credential resource
 git_source {
   url      = "https://github.com/mmiha/challenge.git"
   provider = "gitHub"
   branch   = "main"
 }

 job_cluster {
    new_cluster {
      custom_tags = {
        "ResourceClass" = "SingleNode"
      }

      num_workers   = 0
      spark_version = data.databricks_spark_version.latest.id
      node_type_id  = "Standard_F4"
      spark_conf                   = {
        "spark.databricks.cluster.profile" = "singleNode" 
        "spark.master"                     = "local[*, 4]"
      }

    }
   job_cluster_key = "challenge_job_cluster"
 }

 task {
   task_key = "run_challenge_job" 

   notebook_task {
     notebook_path = "src/job"
   }

   job_cluster_key = "challenge_job_cluster" 

 }
   
  depends_on = [
    azurerm_databricks_workspace.challenge-workspace
  ]

}