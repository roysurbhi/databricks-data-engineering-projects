# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df_read= spark.read.table("fraud_dev.bronze.transactions")

# COMMAND ----------

silver_df = (df_read
            .select(
                "trans_num", "trans_date_trans_time", "trans_date", "cc_num",
                "merchant", "category", "amt", "city", "state", "job",
                "is_fraud", "unix_time"
            )
            .filter(F.col("trans_num").isNotNull())
            .dropDuplicates(["trans_num"])
            .withColumn("_processed_timestamp" , F.current_timestamp())
    

)

# COMMAND ----------

silver_final = (silver_df
                  .write
                  .format("delta")
                  .mode("overwrite")
                  .save(silver_path)
)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS fraud_dev.silver.transactions
    USING DELTA
    LOCATION '{silver_path}'
""")

display(spark.table("fraud_dev.silver.transactions").limit(10))

# COMMAND ----------

print(spark.table("fraud_dev.silver.transactions").count())