// Databricks notebook source
// DBTITLE 1,Doing the Conections with ADLS
// MAGIC %python
// MAGIC spark.conf.set(
// MAGIC     "fs.azure.account.key.dlsturma01imersao.dfs.core.windows.net",
// MAGIC     dbutils.secrets.get(scope="keyvaultzap",key="dlzapKey"))

// COMMAND ----------

// DBTITLE 1,Auth in ADLS
// MAGIC %python
// MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
// MAGIC        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
// MAGIC        "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="keyvaultzap",key="applicationClientId"),
// MAGIC        "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="keyvaultzap",key="secret"),
// MAGIC        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+dbutils.secrets.get(scope="keyvaultzap", key="directoryTenantId")+"/oauth2/token"}

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.secrets.listScopes()
