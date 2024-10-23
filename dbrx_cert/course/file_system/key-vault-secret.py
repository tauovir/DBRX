# Databricks notebook source
# MAGIC %md
# MAGIC ### Secret Scope: 
# MAGIC A secret scope is collection of secrets identified by a name.A workspace is limited to a maximum of 1000 secret scopes.
# MAGIC Follow the below Steps to Create Keyvault and link with Azure Databrciks.
# MAGIC - Go to Azure portal and create key-vault
# MAGIC - Azure-key vault-> Click on secret and create it, If you below error 
# MAGIC **"The operation is not allowed by RBAC. If role assignments were recently changed, please wait several minutes for role assignments to become effective"**.
# MAGIC  then assign the role **Key Vault Secrets Officer** to current User so that it can create secret.
# MAGIC - Go to databrciks and type the _https://<databrciks-Instance>#secrets/createScope_ to open page
# MAGIC - A: Scope Name : enter scope name.
# MAGIC - B: DNS Name : Go to to key vault property. copy Vault URI and paste in DNS Name.
# MAGIC - C: Resource ID :  Go to to key vault property. copy Resource ID and paste here.
# MAGIC - While executing below code If you get permission Denied Error, the Go to Key-Vault add role Assigment,select role **Key Vault Secrets Use** and assign to inbuilt Service Principal **"AzureDatabricks"**
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('key-vault-scope')

# COMMAND ----------

secret = dbutils.secrets.get(scope='key-vault-scope',key = 'test-secret')
if secret == 'test101':
    print("sucess")
else:
    print("Fail")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount The Location

# COMMAND ----------

storage_account_name = "mountstorage101"
container_name = "preprd-datalake"
mount_point = "files"
# Service Principal
client_id = "xxxxxxxx"
tenant_id = "xxxxxxxxx"
client_secret = dbutils.secrets.get(scope='key-vault-scope',key = 'dbrx-clientid')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": f"{client_id}",
        "fs.azure.account.oauth2.client.secret": f"{client_secret}",
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{mount_point}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/files/data/')

# COMMAND ----------

# MAGIC %sql select * from CSV.`/mnt/files/data/diabetes.csv` limit 5

# COMMAND ----------

spark.read.format('csv').load('/mnt/files/data/diabetes.csv').limit(3).display()

# COMMAND ----------

dbutils.fs.mounts()
