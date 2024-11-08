# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC #### Prerequisite Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake. 

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ####Connect ADLS using Service Principal and Keyvault 

# COMMAND ----------

client_id            = dbutils.secrets.get(scope="dataenfproject2-scope", key="ClientID")
tenant_id            = dbutils.secrets.get(scope="dataenfproject2-scope", key="TenantID")
client_secret        = dbutils.secrets.get(scope="dataenfproject2-scope", key="SecretClient")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(storage_account_name,container_name):

    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("dataeng2","raw")

# COMMAND ----------

mount_adls("dataeng2","processed")

# COMMAND ----------

mount_adls("dataeng2","presentation")

# COMMAND ----------

mount_adls("dataeng2","demo")

# COMMAND ----------

