# Databricks notebook source
# MAGIC %md
# MAGIC ### Pipeline's event log.
# MAGIC
# MAGIC ### Zero-config way (event_log)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - event_log('<pipeline-id>') works automatically. DLT always records the event log for every pipeline internally\
# MAGIC event_log('pipeline-id') → built-in, always available, no setup. (What you're using now.)
# MAGIC - A published event-log table (workspace.bronze.wikimedia_event_log) → only exists if you add the event_log block to pipeline settings. That's optional — useful when you want to grant access, join it in other queries, or query it from outside the pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT*-- timestamp, event_type, message
# MAGIC FROM event_log('d5a332ab-559e-49b1-9547-99d46e4f0210') -- pipeline id
# MAGIC ORDER BY timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Data-quality / dropped rows (your expect_or_drop results):

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT timestamp,
# MAGIC   details:flow_progress.data_quality.expectations
# MAGIC FROM event_log('d5a332ab-559e-49b1-9547-99d46e4f0210')
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC   AND details:flow_progress.data_quality IS NOT NULL
# MAGIC ORDER BY timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### event_log vs System tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### event_log('<pipeline-id>') (the TVF you're using)
# MAGIC
# MAGIC - Scoped to one pipeline.
# MAGIC - Most detailed and real-time view — every event: flow_progress, update_progress, data-quality expectations, errors, etc.
# MAGIC - Same underlying data the pipeline UI shows.
# MAGIC - No extra permissions needed beyond access to the pipeline.
# MAGIC
# MAGIC
# MAGIC ### System tables (system.*)
# MAGIC
# MAGIC - Account-wide, governed, historical tables maintained by Databricks across all pipelines/jobs/clusters — e.g. system.billing.usage, system.compute, system.lakeflow.*, system.access.audit.
# MAGIC - Designed for cross-pipeline reporting, cost, and auditing — not the fine-grained per-batch detail.
# MAGIC - May have a slight delay (minutes to hours) vs. the live TVF.
# MAGIC - Need to be enabled by an admin and require permissions.
# MAGIC
# MAGIC  So:
# MAGIC
# MAGIC Want deep detail on this one pipeline (data-quality drops, per-flow progress)? → use event_log('d5a332ab-...').
# MAGIC Want fleet-wide trends, cost, audit history across everything? → use system.* tables.

# COMMAND ----------

files = dbutils.fs.ls("/Volumes/workspace/bronze/landing_zone/wikimedia/")
display(len(files))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.bronze.wikimedia_bronze
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT log_type, event_type, COUNT(*) 
# MAGIC FROM workspace.bronze.wikimedia_bronze
# MAGIC GROUP BY log_type, event_type

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from workspace.bronze.wikimedia_quarantine