# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from faker import Faker
import random
from pyspark.sql import functions as F

fake = Faker()

customer_ids = list(range(1, 211))  # your 210 customers

orders = []
order_id = 1

for customer_id in customer_ids:
    num_orders = random.randint(1, 8)
    for _ in range(num_orders):
        order = {
            "order_id": order_id,
            "customer_id": customer_id,
           "order_date": fake.date_time_between(start_date="-6m", end_date="now"),
            "order_amount": round(random.uniform(10, 500), 2)
        }
        orders.append(order)
        order_id += 1

df_orders = spark.createDataFrame(orders)
print(df_orders.count())
display(df_orders.limit(10))

# COMMAND ----------

df_orders_bronze = (df_orders
    .withColumn("_ingestion_timestamp", F.current_timestamp())
)

df_orders_bronze.write.format("delta").mode("overwrite").save(f"{bronze_path}/orders")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS loyalty_dev.bronze.orders
    USING DELTA
    LOCATION '{bronze_path}/orders'
""")

display(spark.table("loyalty_dev.bronze.orders").limit(10))

# COMMAND ----------

bronze_orders_df = spark.table("loyalty_dev.bronze.orders")

silver_orders_df = (bronze_orders_df
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("order_amount") > 0)
    .withColumn("_processed_timestamp", F.current_timestamp())
)

silver_orders_df.write.format("delta").mode("overwrite").save(f"{silver_path}/orders")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS loyalty_dev.silver.orders
    USING DELTA
    LOCATION '{silver_path}/orders'
""")

display(spark.table("loyalty_dev.silver.orders").limit(10))