# Databricks notebook source
# MAGIC %run  /Workspace/Users/rana@ghazzi.com/Dev/wikimedia/resources/notebooks/params_config

# COMMAND ----------


window= Window.partitionBy("wiki", "title_url").orderBy(col("timestamp").desc())

df = (spark.read.table(f"{catalog}.{bronze_schema}.{bronze_table}")
          .filter((col("bot") == False) & col("type").isin("edit", "new"))
          .select("id", "title", "comment", "title_url", "timestamp", "type", "user", "wiki")
          .withColumn("rn", row_number().over(window))
          .filter("rn = 1")
          .drop("rn")
          .createOrReplaceTempView("wikimedia"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SElect* from wikimedia
# MAGIC where  type = 'new'
# MAGIC --and bot = false

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(id) over( partition by title_url order by timestamp desc) as cnt, (*)
# MAGIC from workspace.bronze.wikimedia_bronze
# MAGIC where bot = false
# MAGIC and type in ('edit','new')
# MAGIC order by title_url,timestamp,id

# COMMAND ----------

from pyspark.sql.functions import col, count, row_number, current_timestamp
from pyspark.sql.window import Window
duplicate_titles = (spark.read
             .table(f"{catalog}.{bronze_schema}.{bronze_table}")
             .filter((col("bot") == False) & (col("type").isin("edit", "new")))
             .select("id", "title", "comment", "title_url", "timestamp", "type", "user", "wiki")
             .groupBy("title","wiki","title_url","timestamp")
             .agg(count("id").alias("count"))
             .filter(col("count") > 1)).display()