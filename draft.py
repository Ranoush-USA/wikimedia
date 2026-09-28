# Databricks notebook source
# MAGIC %sql
# MAGIC LIST '/Volumes/workspace/bronze/landing_zone/wikimedia/';

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.json("/Volumes/workspace/bronze/landing_zone/wikimedia/")
df.display()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("wikimedia")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with rg as (
# MAGIC select count(*) as cnt, title_url
# MAGIC from wikimedia
# MAGIC group by title_url
# MAGIC having count(*) > 1)
# MAGIC
# MAGIC
# MAGIC
# MAGIC select *
# MAGIC from rg left join wikimedia
# MAGIC on rg.title_url = wikimedia.title_url
# MAGIC where rg.cnt > 1
# MAGIC and bot = false
# MAGIC order by rg.title_url

# COMMAND ----------

# MAGIC %sql
# MAGIC select meta.* , title_url
# MAGIC -- * except (meta, revision, length), meta.*, revision.*, length.*
# MAGIC from wikimedia

# COMMAND ----------

# MAGIC %sql
# MAGIC select rng.* except (meta_parsed, meta, length,log_action_comment,log_action_minor,log_action_patrolled,log_action_server_url,log_action_server_name,log_action_server_script_path,log_action_namespace,log_action_title,col1,col2), meta_parsed.*
# MAGIC from (
# MAGIC   select v.*,
# MAGIC          (cast(meta as string), 'struct<domain string, dt string, id string, offset bigint, partition bigint, request_id string, stream string, topic string, uri string>') as meta_parsed
# MAGIC   from wikimedia v
# MAGIC ) rng

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

schema = " comment string, id long, meta struct<domain:string,topic:string,uri:string>, minor boolean, namespace long, notify_url string, parsedcomment string, patrolled boolean,timestamp long, title string, title_url string, type string, user string, wiki string"
df = (spark.read
      .schema(schema)
      .table("workspace.bronze.wikimedia")
      .withColumn("timestamp_silver", col("timestamp").cast("timestamp"))
      .select( "comment", "id", "meta.domain","meta.topic", "meta.uri", "minor", "namespace", "notify_url", "parsedcomment", "patrolled", "timestamp_silver", "title","title_url", "type", "user", "wiki", "timestamp"))
 


df.display()


# COMMAND ----------

# DBTITLE 1,Stream Wikimedia Changes and Collect Events as DataFram ...
import json
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS    = {"User-Agent": "rana-databricks-demo/1.0 (rana@ghazzi.com)"}

def produce(max_events):
    # Read Wikimedia SSE stream, collect events, return as a DataFrame
    resp = requests.get(STREAM_URL, stream=True, timeout=30, headers=HEADERS)
    print("status:", resp.status_code)
    logging.info(f"stream connected, status={resp.status_code}")

    events = []
    for raw_line in resp.iter_lines():
        if not raw_line:
            continue
        decoded = raw_line.decode("utf-8")
        if not decoded.startswith("data: "):
            continue
        payload = decoded[len("data: "):]
        event = json.loads(payload)
        events.append(event)
        if len(events) >= max_events:
            break

    resp.close()
    df = pd.DataFrame(events)
    print(f"[produce] wrote {len(df)} events")
    logging.info(f"produce complete: {len(df)} events collected")
    return df


# COMMAND ----------

df=produce(200)
print(df)