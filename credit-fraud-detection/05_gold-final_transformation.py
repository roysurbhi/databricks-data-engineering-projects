# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

read_df = spark.table("fraud_dev.silver.transactions")

# COMMAND ----------

gold_df = (read_df
    .groupBy("category", "state")
    .agg(
        F.count("trans_num").alias("total_transactions"),
        F.sum("is_fraud").alias("fraud_count"),
        F.round(F.avg("amt"), 2).alias("avg_transaction_amount"),
        F.round(F.sum(F.when(F.col("is_fraud") == 1, F.col("amt")).otherwise(0)), 2).alias("total_fraud_amount")
    )
    .withColumn("fraud_rate_pct", F.round((F.col("fraud_count") / F.col("total_transactions")) * 100, 2))
    .orderBy(F.desc("fraud_rate_pct"))
)

# COMMAND ----------

gold_df.write.format("delta").mode("overwrite").save(f"{gold_path}/fraud_summary")


# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS fraud_dev.gold.fraud_summary
    USING DELTA
    LOCATION '{gold_path}/fraud_summary'
""")

display(spark.table("fraud_dev.gold.fraud_summary"))