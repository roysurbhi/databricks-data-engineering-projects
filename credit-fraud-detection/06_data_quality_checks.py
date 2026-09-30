# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

bronze_df = spark.table("fraud_dev.bronze.transactions")
silver_df = spark.table("fraud_dev.silver.transactions")

bronze_row_count = bronze_df.count()
silver_row_count = silver_df.count()

null_trans_num_count = bronze_df.filter(F.col("trans_num").isNull()).count()

non_null_df = bronze_df.filter(F.col("trans_num").isNotNull())
duplicate_trans_num_count = non_null_df.count() - non_null_df.dropDuplicates(["trans_num"]).count()

invalid_amount_count = silver_df.filter(F.col("amt") <= 0).count()

dq_result = [{
    "run_timestamp": datetime.now().isoformat(),
    "bronze_row_count": bronze_row_count,
    "silver_row_count": silver_row_count,
    "null_trans_num_count": null_trans_num_count,
    "duplicate_trans_num_count": duplicate_trans_num_count,
    "invalid_amount_count": invalid_amount_count,
    "status": "PASS" if invalid_amount_count == 0 else "REVIEW_NEEDED"
}]

dq_df = spark.createDataFrame(dq_result)

dq_path = f"{gold_path}/pipeline_dq_log"
dq_df.write.format("delta").mode("append").save(dq_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS fraud_dev.gold.pipeline_dq_log
    USING DELTA
    LOCATION '{dq_path}'
""")

display(spark.table("fraud_dev.gold.pipeline_dq_log"))