# Databricks notebook source
spark.sql("""
    CREATE CATALOG IF NOT EXISTS loyalty_dev
    MANAGED LOCATION 'abfss://loyalty-analytics-container@loyaltyanalyticsdev.dfs.core.windows.net/catalog-managed'
""")

spark.sql("CREATE SCHEMA IF NOT EXISTS loyalty_dev.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS loyalty_dev.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS loyalty_dev.gold")

print("Catalog and schemas ready")

# COMMAND ----------

storage_account_name = "loyaltyanalyticsdev"
container_name = "loyalty-analytics-container"

raw_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw"
bronze_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze"
silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver"
gold_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold"
checkpoints_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints"

print("Config loaded successfully")