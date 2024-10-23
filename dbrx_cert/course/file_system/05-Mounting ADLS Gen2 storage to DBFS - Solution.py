# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating your DBFS mount for data storage
# MAGIC 1. Create ADLS Gen2 Storage account
# MAGIC 2. Create storage container in your storage account
# MAGIC 3. Create Azure service principal and secret
# MAGIC 4. Grant access to service proncipal for storage account
# MAGIC 5. Mount storage container

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Create ADLS Gen2 Storage account
# MAGIC * Click "Create a resource" on your Azure portal home page
# MAGIC * Search for "Storage account" and click the create button
# MAGIC * Create a storage account using the following
# MAGIC     * Choose an appropriate subscription
# MAGIC     * Select an existing or Create a new Resource group
# MAGIC     * Choose a unique storage account name (Ex prashantsa)
# MAGIC     * Choose a region (Choose the same region where your Databricks service is created)
# MAGIC     * Select performance tier (Standard tier is good enough for learning)
# MAGIC     * Choose storage redundency (LRS is good enough for learning)
# MAGIC     * Click Advanced button to move to the next step
# MAGIC     * Select "Enable hierarchical namespace" on the Advanced tab
# MAGIC     * Click "Review" button
# MAGIC     * Click the "Create" button after reviewing your settings
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Create storage container in your storage account
# MAGIC * Go to your Azure storage account page
# MAGIC * Select "Containers" from the left side menu
# MAGIC * Click "+ Container" button from the top menu
# MAGIC * Give a name to your containe (Ex dbfs-container)
# MAGIC * Click the "Create" button

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Create Azure service principal and secret
# MAGIC * Go to Azure Active Directory Service page in your Azure account (Azure Active Directory is now Microsoft Entra ID)
# MAGIC * Select "App registrations" from the left side menu
# MAGIC * Click (+ New registration) from the top menu
# MAGIC * Give a name to your service principal (Ex databricks-app-principal)
# MAGIC * Click the "Register" button
# MAGIC * Service principal will be created and details will be shown on the service principal page
# MAGIC * Copy "Application (client) ID" and "Directory (tenant) ID" values. You will need them later
# MAGIC * Choose "Certificates & secrets" from the left menu
# MAGIC * Click "+ New client secret" on the secrets page
# MAGIC * Enter a description (Ex databricks-app-principal-secret)
# MAGIC * Select an expiry (Ex 3 Months)
# MAGIC * Click the "Add" button
# MAGIC * Secret will be created and shown on the page
# MAGIC * Copy the Secret value. You will need it later
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Grant access to service proncipal for storage account
# MAGIC * Go to your storage account page
# MAGIC * Click "Access control (IAM)" from the left menu
# MAGIC * Click the "+ Add" button and choose "Add role assignment"
# MAGIC * Search for "Storage Blob Data Contributor" role and select it
# MAGIC * Click "Next" button
# MAGIC * Click the "+ Select members"
# MAGIC * Search for your Databricks service principal (Ex databricks-app-principal) and select it
# MAGIC * Clcik "Select" button
# MAGIC * Click "Review + assign" button twice

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Mount storage container

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.1 Define necessory variables

# COMMAND ----------

storage_account_name = "mountstorage101"
container_name = "preprd-datalake"
mount_point = "files"
client_id = ""
tenant_id = ""
client_secret =""

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.2 Define mount configs
# MAGIC You can follow the instruction and code sample from below documentation page
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts#--mount-adls-gen2-or-blob-storage-with-abfs

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": f"{client_id}",
        "fs.azure.account.oauth2.client.secret": f"{client_secret}",
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.3 Mount the container

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{mount_point}",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.4. List contents of your mount point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.5. Upload your sample data folder to your mounted location

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.6. List contents of your mount point

# COMMAND ----------

dbutils.fs.ls('/mnt/files/data/')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from CSV.`/mnt/files/data/diabetes.csv`

# COMMAND ----------


spark.read.format('csv').load('/mnt/files/data/diabetes.csv').display()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.7. Unmount /mnt/files directory

# COMMAND ----------

dbutils.fs.unmount('/mnt/files')

# COMMAND ----------

# MAGIC %fs
# MAGIC unmount /mnt/files

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Secret Scope
# MAGIC -  https://<databrciks-Instance>#secrets/createScope
# MAGIC The Secret access Permission are as follows:
# MAGIC - **MANAGE** :  Allowed to change ACLs, and read and write to this secrets/createScoppe
# MAGIC - **Write**: Allowed to read and write the secrets/createScoppe
# MAGIC - **Read** :  Allowed to read thi secret scope and list what secret are available.

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('key-vault-scope')

# COMMAND ----------

secret = dbutils.secrets.get(scope='key-vault-scope',key = 'dbrx-clientid')

# COMMAND ----------

if secret == 's6n8Q~_XmnrHzqMoNQ~4b_B6O6FGIp2ifNTDFcP833':
    print("sucess")
else:
    print("ail")

# COMMAND ----------

dbutils.secrets.help()
