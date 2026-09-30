# Databricks notebook source
storage_account_name = "creditcardtxnsurbhi"
container_name = "credit-card-transaction-container"

raw_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw"
bronze_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze"
silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver"
gold_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold"
checkpoints_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints"


# COMMAND ----------

spark.sql("""
    CREATE CATALOG IF NOT EXISTS fraud_dev
    MANAGED LOCATION 'abfss://credit-card-transaction-container@creditcardtxnsurbhi.dfs.core.windows.net/catalog-managed'
""")

spark.sql("CREATE SCHEMA IF NOT EXISTS fraud_dev.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS fraud_dev.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS fraud_dev.gold")
