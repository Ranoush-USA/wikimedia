# WIKIMEDIA

Real-time Wikimedia event data pipeline built as a Declarative Automation Bundle (DAB). The pipeline ingests live edit events from the [Wikimedia RecentChanges SSE stream](https://stream.wikimedia.org/v2/stream/recentchange), processes them through a medallion architecture (Bronze → Silver), and stores curated data in Delta tables for analytics.

## Architecture

```
Wikimedia SSE Stream
        │
        ▼
┌─────────────────────┐
│  Producer            │  Reads SSE stream, writes one JSON file per event
│  (landing zone)      │  to UC Volume: /Volumes/workspace/bronze/landing_zone/wikimedia/
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Bronze Layer        │  Auto Loader (cloudFiles) streams JSON from landing zone
│  wikimedia_bronze   │  into Delta table with trigger(availableNow=True)
└─────────┬───────────┘
          │  Change Data Feed (CDF) enabled
          ▼
┌─────────────────────┐
│  Silver Layer        │  Reads Bronze via CDF, filters bots & non-edit events,
│  wikimedia_silver    │  deduplicates by (wiki, title), MERGE upserts into Silver
└─────────────────────┘
```

## Project Structure

```
wikimedia/
├── databricks.yml                    # Bundle configuration (variables, targets, includes)
├── README.md
├── .gitignore
├── resources/
│   ├── jobs/
│   │   └── wikimedia.yml              # Job definition: WIKIMEDIA (3 sequential tasks)
│   ├── pipelines/                     # (reserved for future SDP pipelines)
│   └── notebooks/
│       ├── params_config              # Shared config: paths, schema, widget parameters
│       ├── Producer_streaming_engine.ipynb  # SSE stream consumer → landing zone
│       ├── wikimedia_bronze.ipynb     # Auto Loader ingestion → Bronze Delta table
│       └── wikimedia_silver.ipynb     # CDF read + dedup + MERGE → Silver Delta table
├── WIKIMEDIA                          # Main orchestration notebook
├── Monitoring & DQ                     # Data quality monitoring notebook
├── QA                                 # QA / testing notebook
└── draft                             # Scratch / experimental notebook
```

## Job: WIKIMEDIA

The bundle deploys a single job (`WIKIMEDIA`) with three sequential tasks:

| Order | Task Key | Notebook | Description |
| --- | --- | --- | --- |
| 1 | `WIKIMEDIA-API-Ingestion` | `Producer_streaming_engine.ipynb` | Consumes Wikimedia SSE stream, writes JSON events to landing zone |
| 2 | `Auto_Loader_bronze` | `wikimedia_bronze.ipynb` | Auto Loader incrementally loads JSON into Bronze Delta table |
| 3 | `Silver_CDC_Merge` | `wikimedia_silver.ipynb` | Deduplicates and MERGE-upserts curated events into Silver Delta table |

- **Performance target**: `PERFORMANCE_OPTIMIZED`
- **Queue**: enabled
- **Email notifications**: sent to `rana@ghazzi.com` on success and failure

## Configuration

### Bundle Variables

| Variable | Default | Description |
| --- | --- | --- |
| `catalog` | `workspace` | Unity Catalog catalog |
| `bronze_schema` | `bronze` | Schema for Bronze layer |
| `silver_schema` | `silver` | Schema for Silver layer |
| `gold_schema` | `gold` | Schema for Gold layer (reserved) |

### Targets

| Target | Mode | Default |
| --- | --- | --- |
| `dev` | development | yes |
| `prod` | production | no |

### Shared Parameters (`params_config`)

The `params_config` notebook defines source paths, checkpoint locations, the JSON schema, and widget parameters (`catalog`, `bronze_schema`, `silver_schema`, `gold_schema`, `bronze_table`, `silver_table`) consumed by both Bronze and Silver notebooks via `%run`.

### Key Paths

- **Landing zone**: `/Volumes/workspace/bronze/landing_zone/wikimedia/`
- **Checkpoint (bronze)**: `/Volumes/workspace/bronze/checkpoints/wikimedia`
- **Checkpoint (silver)**: `/Volumes/workspace/bronze/checkpoints/wikimedia/checkpoint`
- **Bronze table**: `workspace.bronze.wikimedia_bronze`
- **Silver table**: `workspace.silver.wikimedia_silver`

## Getting Started

### Deploy the Bundle

Click the **deployment rocket** 🚀 in the left sidebar to open the **Deployments** panel, then click **Deploy**. Alternatively, use the CLI:

```bash
databricks bundle deploy --target dev
```

### Run the Job

Hover over the `WIKIMEDIA` job in the **Deployments** panel and click **Run**, or use the CLI:

```bash
databricks bundle run WIKIMEDIA --target dev
```

### Validate the Bundle

```bash
databricks bundle validate --strict --target dev
```

## Documentation

- [Declarative Automation Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- [Declarative Automation Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
- [Wikimedia EventStreams](https://stream.wikimedia.org/v2/stream/recentchange)
- [Databricks Auto Loader](https://docs.databricks.com/aws/en/ingestion/auto-loader/)
- [Delta Lake Change Data Feed](https://docs.databricks.com/aws/en/delta/delta-change-feed)
