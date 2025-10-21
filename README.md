# 882-f25-class-project-team9: MBTA & Bluebikes Integration Pipeline
## Team Members
Yu Xu, Nilay Jaini, Shuomeng Guan, YiFeng Chen
## Project Overview
This project builds a real-time data pipeline integrating MBTA transit data with Bluebikes bike-share data to analyze "last-mile" transportation effectiveness in the Boston area. Our goal is to evaluate how well Bluebikes complements MBTA service by examining station proximity, availability patterns, and usage coordination between the two systems.
Key Question: Is the last mile being served effectively?
## Architecture Overview
### MBTA Pipeline(EtLT)
MBTA v3 API → Airflow (every 2 min) → GCS Raw Zone → BigQuery Staging → BigQuery Base Tables → Dashboard

**Data Sources:**
- `/routes` - Route information
- `/predictions` - Real-time trip-stop predictions
- `/vehicles` - Real-time vehicle positions

**Key Tables:**
- `rt_trip_stop_predictions` - Prediction events with arrival/departure times
- `rt_vehicle_positions` - Vehicle location snapshots
- Derived dimension views: `dim_route_v`, `dim_stop_v`, `dim_trip_v`, `dim_vehicle_v`

**Orchestration:** Apache Airflow DAG (`mbta_realtime_to_bigquery`) runs every 2 minutes, extracting data to GCS, loading to staging tables, and merging into partitioned base tables.

### Bluebikes Pipeline (ELT)
```
Bluebikes GBFS API → Cloud Function (every 5 min) → GCS → Cloud Function → BigQuery Staging → BigQuery Transformed Tables
```

**Data Sources:**
Initially evaluated 6 tables from the GBFS API:
- `ebikes_at_stations` (no data)
- `system_information` (no data)
- `station_information` ✓
- `station_status` ✓
- `free_bike_status` (no data)
- `system_regions` ✓

Only 3 tables contained active data and were selected for the pipeline.

**Key Tables:**
- `station_status` - Real-time bike and dock availability (Fact Table)
- `station_info` - Static station details with merged region information (Dimension Table)
- Bluebikes historical data for trajectory analysis

**Transformations:**
- Dropped irrelevant columns (e.g., `legacy_id`)
- Consolidated `rental_methods` into comma-delimited format
- Merged `system_regions` into `station_information` for unified dimension table

**Automation:** Pub/Sub triggered Cloud Functions automatically ingest data every 5 minutes and load into BigQuery staging tables.
## Technology Stack

| Component | Technology |
|-----------|-----------|
| **Orchestration** | Apache Airflow (MBTA), Cloud Functions + Pub/Sub (Bluebikes) |
| **Raw Storage** | Google Cloud Storage |
| **Data Warehouse** | BigQuery |
| **Visualization** | Tableau (Phase 1), Streamlit (Phase 2) |
| **APIs** | MBTA v3 API, Bluebikes GBFS API |

## Design Decisions

**Why EtLT for MBTA?**
- Light transformation in Python (key generation, timestamp normalization)
- Heavy transformation in BigQuery SQL (deduplication, MERGE operations)
- Leverages BigQuery's columnar storage and elasticity

**Why ELT for Bluebikes?**
- Raw ingestion to BigQuery staging
- All transformations in BigQuery after load
- Simpler Cloud Function logic

**Why separate orchestration approaches?**
- MBTA: Complex multi-endpoint workflow benefits from Airflow's DAG structure
- Bluebikes: Simple single-source ingestion works well with event-driven Cloud Functions

**Why GCS as raw zone?**
- Low-cost object storage
- Timestamped paths enable data lineage and replay
- Native integration with BigQuery loading

## Data Quality & Validation

**MBTA:**
- SHA1-based deduplication keys prevent duplicate records
- Timestamp normalization to UTC
- Partition pruning by `DATE(observed_at_utc)`
- Predictions filtered: `stop_id IS NOT NULL`
- Service date calculation: `observed_at_utc - 3 hours` (MBTA rule)

**Bluebikes:**
- Discovered empty tables during initial assessment
- Selected only populated tables for pipeline
- Column-level cleanup for analysis readiness

## Current Analytics & Insights
Phase 1 Visualizations (Tableau)
### MBTA:

Top 10 Start Stations by trip volume
Trips by Hour of Day showing peak commute patterns
Peak usage: 7-9 AM and 4-6 PM

### Bluebikes:

Start/End Station Top 10 analysis
Week Day Analysis: Member vs. Casual usage patterns
Ride by Season breakdown
Total rides in 2025: 3.57M

### Key Findings:

- Complementary usage patterns between transit and bike-share
- High Bluebikes activity near major MBTA stations
- Seasonal variation in bike usage

## Project Links
- Jira Board: https://kathyxu.atlassian.net/jira/software/projects/SCRUM/boards/1/backlog?atlOrigin=eyJpIjoiMzc3NzllZmRkYjgwNGEwM2EyNDAyMDQ0YmUxMjZhYTIiLCJwIjoiaiJ9
- Google Slides: https://docs.google.com/presentation/d/1Omhc5fMa41FHkgaKSywdfcyPfr950P-EY7PUNzjB724/edit?usp=sharing
- Github: https://github.com/Kathyuxu/882-f25-class-project

## Data Sources
- MBTA Developer Portal: https://www.mbta.com/developers
- Bluebikes System Data: https://bluebikes.com/system-data



