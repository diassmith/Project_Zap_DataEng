// Databricks notebook source
/*import org.apache.spark.sql.functions.{get_json_object,to_timestamp}*/

val inboundFile = "adl://dlszap.blob.core.windows.net/bronze/source-4-ds-train.json"
val bronzeDF = spark.read.text(inboundFile)

// COMMAND ----------

display(dbutils.fs.ls("https://dlszap.dfs.core.windows.net/"))
