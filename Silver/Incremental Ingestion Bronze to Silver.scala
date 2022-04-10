// Databricks notebook source
// DBTITLE 1,Importing Libraries
import org.apache.spark.sql.functions.get_json_object
import org.apache.spark.sql.functions.{get_json_object,to_timestamp}

// COMMAND ----------

// DBTITLE 1,Reading the Json File and Creating the column
val bronzeFile = "dbfs:/mnt/bronze/source-4-ds-train.json"
val silverDF = spark.read.text(bronzeFile)
val silverDF2 = silverDF.withColumn("id",get_json_object($"value","$.id"))
                        /*Using to_timestamp converting STRING to TIME*/
                        .withColumn("createdAt", to_timestamp(get_json_object($"value","$.createdAt")))
                        .withColumn("updatedAt", to_timestamp(get_json_object($"value","$.updatedAt")))


// COMMAND ----------

// DBTITLE 1,Checking if the IDs are unique
silverDF.select(get_json_object($"value","$.id") as "id").groupBy("id").count().orderBy($"count".desc).show()

// COMMAND ----------

// DBTITLE 1,Showing the Json File
display(silverDF)

// COMMAND ----------

// DBTITLE 1,Environment variables
val dbName = "db_zap_silver"
val tbName = "tb_zap_imoveis"
val tableId = s"$dbName.$tbName"

// COMMAND ----------

// DBTITLE 1,Showing the tableId
tableId

// COMMAND ----------

// DBTITLE 1,Creating database if not exists
// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS db_zap_silver

// COMMAND ----------

// DBTITLE 1,DeltaLake - Incremental Bronze to Silver
/* If the table not exists, create the table with the value in variable and INSERT all tables*/
if(!spark.catalog.tableExists(tableId)) {
  silverDF2.write
           .format("delta")/*using the DELTA Format*/
           .mode("append")/* using the "append" mode to bring the last data*/
           .option("path","dbfs:/mnt/silver")/* url from the table*/
           .saveAsTable(tableId)/*which table would you like to save your data*/
} else {/* If the table exist, */
 silverDF2.createOrReplaceTempView("vw_source")
 spark.sql(s"""
   MERGE INTO ${tableId} as target
   USING vw_source as source 
   ON target.id = source.id 
   WHEN MATCHED AND source.updatedAt > target.updatedAt THEN 
    UPDATE SET *
   WHEN NOT MATCHED THEN
     INSERT *
 """)
}

// COMMAND ----------

display(silverDF2)

// COMMAND ----------

/*spark.sql("CREATE OR REPLACE TEMPORARY VIEW Json USING json OPTIONS" + 
      "(path 'dbfs:/mnt/bronze/source-4-ds-train.json')")
spark.sql("select id from Json").show()*/
