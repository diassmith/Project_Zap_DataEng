// Databricks notebook source
// DBTITLE 1,Doing the Conections with ADLS
spark.conf.set(
    "fs.azure.account.key.dlsturma01imersao.dfs.core.windows.net",
    dbutils.secrets.get(scope="keyvaultzap",key="dlzapKey"))

// COMMAND ----------

// DBTITLE 1,Auth in ADLS

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="keyvaultzap",key="applicationClientId"),
       "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="keyvault",key="secret"),
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+dbutils.secrets.get(scope="keyvault", key="directoryTenantId")+"/oauth2/token"}

/*testando 

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.secrets.listScopes()
