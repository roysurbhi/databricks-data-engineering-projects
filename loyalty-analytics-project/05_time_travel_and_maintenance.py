# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel — querying customer_dim as it looked before vs. after your MERGE

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY loyalty_dev.silver.customer_dim"))

# COMMAND ----------

df_v0 = spark.sql("SELECT * FROM loyalty_dev.silver.customer_dim VERSION AS OF 0")
print(df_v0.count())

# COMMAND ----------

df_current = spark.table("loyalty_dev.silver.customer_dim")
print(df_current.count())

# COMMAND ----------

print("At version 0 (before any changes):")
spark.sql("SELECT * FROM loyalty_dev.silver.customer_dim VERSION AS OF 0 WHERE customer_id = 65").show()

print("Current state:")
spark.table("loyalty_dev.silver.customer_dim").filter("customer_id = 65").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### #OPTIMIZE — compacts small files into larger ones

# COMMAND ----------

result = spark.sql("OPTIMIZE loyalty_dev.silver.customer_dim")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM (dry run, safe)

# COMMAND ----------

spark.sql("VACUUM loyalty_dev.silver.customer_dim DRY RUN").show(truncate=False)