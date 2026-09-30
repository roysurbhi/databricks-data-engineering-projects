# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df_train = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{raw_path}/fraudTrain.csv"))

# COMMAND ----------

df_test = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{raw_path}/fraudTest.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION OF df_train and df_test by unionByName

# COMMAND ----------

df_all = df_train.unionByName(df_test)



# COMMAND ----------

display(df_all.limit(5))

# COMMAND ----------

print(df_train.count())
print(df_test.count())
print(df_all.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a new column "trans_date" by extracting just the date portion (no time) from trans_date_trans_time.

# COMMAND ----------

df_all = df_all.withColumn("trans_date", F.to_date(F.col("trans_date_trans_time")))

# COMMAND ----------

df_all.select("trans_date_trans_time", "trans_date").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC 1.Get just the trans_date column, with duplicates removed
# MAGIC 2.Sort those dates in order
# MAGIC 3.Keep only the first 20 (or 30, your choice)
# MAGIC 4.Pull those into a plain Python list
# MAGIC 5.Filter your main df_all DataFrame to keep only rows matching those dates

# COMMAND ----------

dates_df = df_all.select("trans_date").dropDuplicates().orderBy("trans_date").limit(20)

sample_dates = [row["trans_date"] for row in dates_df.collect()]

df_sample = df_all.filter(F.col("trans_date").isin(sample_dates))

# COMMAND ----------

print(df_sample.select("trans_date").distinct().count())  # should print 20
print(df_sample.count())  # total row count across those 20 days

# COMMAND ----------

print(df_sample.select("trans_date").distinct().count())

# COMMAND ----------

(df_sample
.write
.partitionBy("trans_date")
.mode("overwrite")
.option("header", True)
.csv(f"{raw_path}/transactions/"))

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_path}/transactions/"))

# COMMAND ----------

all_dates_sorted = sorted(df_all.select("trans_date").distinct().collect())
new_day_date = all_dates_sorted[20]

df_new_day = df_all.filter(F.col("trans_date") == new_day_date[0])

(df_new_day
    .write
    .partitionBy("trans_date")
    .mode("append")
    .option("header", True)
    .csv(f"{raw_path}/transactions/"))

print(f"Added new day: {new_day_date}")

# COMMAND ----------

folders = dbutils.fs.ls(f"{raw_path}/transactions/")
date_folders = [f for f in folders if f.name.startswith("trans_date=")]
print(len(date_folders))