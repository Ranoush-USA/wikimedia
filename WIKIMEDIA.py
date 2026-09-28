# Databricks notebook source
df=spark.read.json("/Volumes/workspace/bronze/landing_zone/wikimedia/event_20260606_000142_956324_1.json")
df.printSchema()

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, col, get_json_object

SOURCE_PATH = "/Volumes/workspace/bronze/landing_zone/wikimedia/"

schema = " schema string, bot boolean, comment string, id long, length struct<old: bigint, new: bigint>, new long, old long, log_action string, log_action_comment string, log_id long, log_params string, log_type string, meta struct<domain: string, dt: string, id: string, offset: bigint, partition: bigint, request_id: string, stream: string, topic: string, uri: string>, minor boolean, namespace long, notify_url string, parsedcomment string, patrolled boolean, revision struct<new: bigint, old: bigint>, server_name string, server_script_path string, server_url string, timestamp long, title string, title_url string, type string, user string, wiki string"

def read_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(SOURCE_PATH)
        .withColumn("_ingest_time", current_timestamp())
        .col("_metadata.file_path").alias("_source_file"))

@dlt.table(name="wikimedia_bronze",comment="Raw wikimedia recentchange events — valid rows")
@dlt.expect_or_drop("valid_event_type", "event_type IS NOT NULL")
def wikimedia_bronze():
    return read_raw()

@dlt.table(
    name="wikimedia_quarantine",
    comment="Rows with missing event_type"
)
def wikimedia_quarantine():
    return read_raw().filter(col("event_type").isNull())