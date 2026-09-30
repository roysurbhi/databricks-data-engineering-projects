# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Autoloader read 

# COMMAND ----------

df_read = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", f"{checkpoints_path}/transactions_schema")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("header", True)
      .load(f"{raw_path}/transactions/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add audit columns

# COMMAND ----------

df_bronze = (df_read
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingestion_timestamp", F.current_timestamp()))

# COMMAND ----------

bronze_final=(df_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoints_path}/transactions_bronze")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("fraud_dev.bronze.transactions"))

# COMMAND ----------

display(spark.table("fraud_dev.bronze.transactions").limit(10))

# COMMAND ----------

print(spark.table("fraud_dev.bronze.transactions").count())