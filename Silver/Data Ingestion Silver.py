# Databricks notebook source
# DBTITLE 1,Doing the Conections with ADLS
spark.conf.set(
    "fs.azure.account.key.dlszap.dfs.core.windows.net",
    dbutils.secrets.get(scope="keyvaultzap",key="dlzapKey"))

# COMMAND ----------

# DBTITLE 1,Checking all scopes created
# MAGIC %python
# MAGIC dbutils.secrets.listScopes()

# COMMAND ----------

# DBTITLE 1,Auth in ADLS
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="keyvaultzap",key="applicationClientId"),
       "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="keyvaultzap",key="secret"),
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+dbutils.secrets.get(scope="keyvaultzap", key="directoryTenantId")+"/oauth2/token"}

# COMMAND ----------

# DBTITLE 1, Create mount point on layers
zones = ["bronze", "silver", "gold"]
for zone in zones:
    try:
        dbutils.fs.mount(
            source = "abfss://"+zone+"@dlszap.dfs.core.windows.net/",
            mount_point = "/mnt/"+zone+"/",
            extra_configs = configs)
    except:
            print("")

# COMMAND ----------

# DBTITLE 1,Check mount CLI Linux
# MAGIC %fs ls /mnt/bronze/

# COMMAND ----------

# DBTITLE 1,Check mount with Python
dbutils.fs.ls ("/mnt/bronze")

# COMMAND ----------

# MAGIC %scala
# MAGIC val bronzeFile = "dbfs:/mnt//bronze/source-4-ds-train.json"
# MAGIC val silverDF = spark.read.text(bronzeFile)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(silverDF)
