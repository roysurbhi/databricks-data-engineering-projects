# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

customer_window = (Window
    .partitionBy("customer_id")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
customer_window_all = Window.partitionBy("customer_id")

# COMMAND ----------

silver_orders_df = spark.table("loyalty_dev.silver.orders")

orders_with_running_total = (silver_orders_df
    .withColumn("running_total", F.sum("order_amount").over(customer_window))
    .withColumn("total_order",F.count("order_id").over(customer_window_all))
)

display(orders_with_running_total.orderBy("customer_id", "order_date").limit(20))

# COMMAND ----------

display(orders_with_running_total.orderBy("customer_id", "order_date").limit(10))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

customer_totals = (silver_orders_df
    .groupBy("customer_id")
    .agg(F.sum("order_amount").alias("total_spend"))
)

rank_window = Window.orderBy(F.desc("total_spend"))

customer_ranked = (customer_totals
    .withColumn("spend_rank", F.rank().over(rank_window))
)

display(customer_ranked.orderBy("spend_rank").limit(20))

# COMMAND ----------

lag_window = (Window
    .partitionBy("customer_id")
    .orderBy("order_date")
)

orders_with_lag = (silver_orders_df
    .withColumn("previous_order_amount", F.lag("order_amount", 1).over(lag_window))
    .withColumn("amount_change", F.col("order_amount") - F.col("previous_order_amount"))
)

display(orders_with_lag.orderBy("customer_id", "order_date").limit(20))