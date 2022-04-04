// Databricks notebook source
// DBTITLE 1,Importing Libraries

import org.apache.spark.sql.functions.get_json_object
import org.apache.spark.sql.functions.{get_json_object,to_timestamp}

// COMMAND ----------

// DBTITLE 1,Checking if the IDs are unique
silverDF.select(get_json_object($"value","$.id") as "id").groupBy("id").count().orderBy($"count".desc).show()

// COMMAND ----------

// DBTITLE 1,Reading the Json File and Creating the column
val bronzeFile = "dbfs:/mnt/bronze/source-4-ds-train.json"
val silverDF = spark.read.text(bronzeFile)
val silverDF2 = silverDF.withColumn("id",get_json_object($"value","$.id"))
                        .withColumn("createdAt", to_timestamp(get_json_object($"value","$.createdAt")))
                        .withColumn("updatedAt", to_timestamp(get_json_object($"value","$.updatedAt")))


// COMMAND ----------

// DBTITLE 1,Showing the Json File
display(silverDF)

// COMMAND ----------

spark.sql("CREATE OR REPLACE TEMPORARY VIEW Json USING json OPTIONS" + 
      "(path 'dbfs:/mnt/bronze/source-4-ds-train.json')")
spark.sql("select id from Json").show()

// COMMAND ----------


