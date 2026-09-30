# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Cell 4 — then this
from faker import Faker
import random
from pyspark.sql import functions as F

fake = Faker()

# COMMAND ----------

num_customers = 200
loyalty_tiers = ["Bronze", "Silver", "Gold", "Platinum"]

customers_v1 = []
for i in range(1, num_customers + 1):
    customer = {
        "customer_id": i,
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "loyalty_tier": random.choice(loyalty_tiers),
        "signup_date": fake.date_between(start_date="-2y", end_date="-6m"),
        "snapshot_date": "2024-01-01"
    }
    customers_v1.append(customer)

df_customers_v1 = spark.createDataFrame(customers_v1)
display(df_customers_v1.limit(10))

# COMMAND ----------

import copy

customers_v2 = copy.deepcopy(customers_v1)

# pick 25 random existing customers to modify
customers_to_change = random.sample(customers_v2, 25)

for customer in customers_to_change:
    customer["city"] = fake.city()
    customer["state"] = fake.state_abbr()
    customer["loyalty_tier"] = random.choice(loyalty_tiers)

# generate 10 brand new customers
for i in range(num_customers + 1, num_customers + 11):
    new_customer = {
        "customer_id": i,
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "loyalty_tier": random.choice(loyalty_tiers),
        "signup_date": fake.date_between(start_date="-3m", end_date="today"),
        "snapshot_date": "2024-02-01"
    }
    customers_v2.append(new_customer)

# update snapshot_date for everyone in this batch
for customer in customers_v2:
    customer["snapshot_date"] = "2024-02-01"

df_customers_v2 = spark.createDataFrame(customers_v2)
display(df_customers_v2.limit(10))

# COMMAND ----------

print(len(customers_v1))
print(len(customers_v2))

# COMMAND ----------

changed_count = 0
for i in range(200):
    if customers_v1[i]["city"] != customers_v2[i]["city"] or customers_v1[i]["state"] != customers_v2[i]["state"]:
        changed_count += 1
print(f"Number of customers with different city/state: {changed_count}")

# COMMAND ----------

(df_customers_v1
      .write
      .format("delta")
      .mode("overwrite")
      .save(f"{bronze_path}/customers_v1")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS loyalty_dev.bronze.customers_v1
    USING DELTA
    LOCATION '{bronze_path}/customers_v1'
""")

(df_customers_v2
        .write
        .format("delta")
        .mode("overwrite")
        .save(f"{bronze_path}/customers_v2")
)
        
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS loyalty_dev.bronze.customers_v2
    USING DELTA
    LOCATION '{bronze_path}/customers_v2'
""")

print("Both customer snapshots saved")