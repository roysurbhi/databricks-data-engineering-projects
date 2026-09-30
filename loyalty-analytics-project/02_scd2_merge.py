# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

v1_df = spark.table("loyalty_dev.bronze.customers_v1")

customer_dim = (v1_df
    .withColumn("effective_date", F.col("snapshot_date"))
    .withColumn("end_date", F.lit(None).cast("string"))
    .withColumn("is_current", F.lit(True))
)

(customer_dim
        .write
        .format("delta")
        .mode("overwrite")
        .save(f"{silver_path}/customer_dim")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS loyalty_dev.silver.customer_dim
    USING DELTA
    LOCATION '{silver_path}/customer_dim'
""")

display(spark.table("loyalty_dev.silver.customer_dim"))

# COMMAND ----------

spark.sql(f"""
MERGE INTO loyalty_dev.silver.customer_dim AS target
USING loyalty_dev.bronze.customers_v2 AS source
ON target.customer_id = source.customer_id AND target.is_current = true

WHEN MATCHED AND (
    target.city != source.city OR
    target.state != source.state OR
    target.loyalty_tier != source.loyalty_tier
)
THEN UPDATE SET
    target.end_date = source.snapshot_date,
    target.is_current = false
""")

# COMMAND ----------

spark.sql(f"""
INSERT INTO loyalty_dev.silver.customer_dim
(city, customer_id, email, loyalty_tier, name, signup_date, snapshot_date, state, effective_date, end_date, is_current)
SELECT
    source.city, source.customer_id, source.email, source.loyalty_tier, source.name,
    source.signup_date, source.snapshot_date, source.state,
    source.snapshot_date AS effective_date,
    NULL AS end_date,
    true AS is_current
FROM loyalty_dev.bronze.customers_v2 AS source
WHERE NOT EXISTS (
    SELECT 1 FROM loyalty_dev.silver.customer_dim AS target
    WHERE target.customer_id = source.customer_id
    AND target.is_current = true
)
""")

# COMMAND ----------

print(spark.table("loyalty_dev.silver.customer_dim").count())

# COMMAND ----------

display(spark.table("loyalty_dev.silver.customer_dim").filter("customer_id = 65").orderBy("effective_date"))

# COMMAND ----------

display(spark.table("loyalty_dev.silver.customer_dim").groupBy("customer_id").count().filter("count > 1"))