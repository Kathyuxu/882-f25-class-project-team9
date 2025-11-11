# 882-f25-class-project-team9: MBTA & Bluebikes Integration Pipeline
## Team Members
Yu Xu, Nilay Jaini, Shuomeng Guan, YiFeng Chen
## Project Overview
This project builds a real-time data pipeline integrating MBTA transit data with Bluebikes bike-share data to analyze "last-mile" transportation effectiveness in the Boston area. Our goal is to evaluate how well Bluebikes complements MBTA service by examining station proximity, availability patterns, and usage coordination between the two systems.
Key Question: Is the last mile being served effectively?
## Phase 1

## Phase 2 Updates
Phase 2 migrated all data pipelines to Astronomer Cloud (Airflow), implemented machine learning models for predictive analytics and transit desert identification, and deployed a comprehensive Streamlit dashboard for interactive visualization and real-time monitoring.

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

**Orchestration:** Astronomer Airflow DAG (`mbta_realtime_to_bigquery`) runs every 2 minutes, extracting data to GCS, loading to staging tables, and merging into partitioned base tables.

### Bluebikes Pipeline (ELT)
```
Bluebikes GBFS API → bluebikes_gbfs_to_gcs(updated every 2 mins) → GCS → bluebikes_stations_status_from_gcs → BigQuery Staging → BigQuery Transformed Tables
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
| **Orchestration** | Astronomer Airflow (MBTA), Cloud Functions|
| **Raw Storage** | Google Cloud Storage |
| **Data Warehouse** | BigQuery |
| **Visualization** | Streamlit |
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
  
## Machine Learning
Using Airflow through Astronomer 

### MBTA:

#### MBTA Occupancy Prediction (BigQueryML)
**Model:** Logistic Regression with AUTO_CLASS_WEIGHTS=TRUE, MAX_ITERATIONS=50

**Pipeline:** Weekly Airflow DAG (`occ_train_weekly`, Mon 05:00) orchestrates end-to-end feature engineering, training, and evaluation.

**Features:**
- `hour_of_day`, `day_of_week`, `is_weekend`
- `lat_bin`, `lon_bin` (coarse geo-bins)
- `route_id`
- `current_status`

**Training Window:** Rolling window from T-67d to T-7d, evaluated on last 7 days

**Results:**
- Fast convergence with class weighting mitigating imbalance
- Route and time-of-day are strongest signals
- Coarse geo-bins improve generalization

**Model Artifacts:**
- `mbta_ml.occ_features_min`
- `mbta_ml.occ_lr_min`
- `mbta_ml.occ_eval_last`
### Bluebikes 
#### Transit Desert Clustering
**Model:** K-means Clustering

**Purpose:** Identify MBTA stations with poor Bluebikes connectivity

**Features (10 engineered):**
- Spatial proximity (distance to nearest Bluebikes stations)
- Temporal availability (bike availability during peak hours)
- Ridership patterns (MBTA usage + Bluebikes demand)

**Current Status:**
- Dashboard displays 514 observations (~3-4x actual 153 stations) due to insufficient deduplication
- Classification logic currently labels all stations as "Transit Deserts"
- Threshold-based criteria (>800m walking distance OR <2 morning bikes) poorly calibrated for Boston's density

**Phase 3 Improvements:**
1. Implement proper run versioning to display only most recent results
2. Transition from absolute thresholds to percentile-based classification
3. Validate labels against ground-truth assessments of known well-served/underserved stations
 
#### Demand Prediction
**Model:** Linear Regression

**Purpose:** Predict future Bluebikes demand for bike redistribution and station planning

**Output:** Predictions stored in BigQuery ML, displayed in Streamlit

**Use Case:** Identify when and where demand peaks occur for proactive rebalancing

**Integration:** Fully automated through Airflow pipeline with scheduled training and updates

## Streamlit Dashboard

Interactive front-end bringing together all pipeline outputs with real-time updates via Airflow DAGs and BigQuery.

### Dashboard Pages

**1. Review_MBTA**
- Historical system performance
- Basic Analysis

**2. Review_BB**
- Historical system performance
- Top Bluebikes start/end stations
- Ridership distribution across Boston
  
**3. Overview**
- Geo Analysis of stations meeting "Last-mile" requirement
- Maps for Visualization
  
**4. Summary**
- Last-mile coverage evaluation
- 333 out of 514 MBTA stations meet one-mile rule
- 181 stations do not meet criteria
- Detailed distance statistics

**5. Recommendations**
- Short-, medium-, and long-term strategies
- Pilot location suggestions for new stations

**6. Occupancy Prediction**
- MBTA vehicle crowding forecasts (BigQuery ML logistic regression)
- Likelihood of high/low occupancy by route and hour
- Real-time less-crowded travel options
  
**7. Bluebikes Clustering**
- K-Means model visualization
- "Transit desert" identification
- Poor connectivity areas highlighted

**8. Bluebikes Demand Prediction**
- Predicted pickups and drop-offs by station
- Highest forecasted usage stations
- Time period analysis

**9. Availability**
- Search any MBTA station
- View nearest Bluebikes docks instantly
- Real-time bike availability




## Project Links

- **GitHub:** https://github.com/Kathyuxu/882-f25-class-project-team9.git
- **Jira Board:** [Project Backlog](https://kathyxu.atlassian.net/jira/software/projects/SCRUM/boards/1/backlog?atlOrigin=eyJpIjoiMzc3NzllZmRkYjgwNGEwM2EyNDAyMDQ0YmUxMjZhYTIiLCJwIjoiaiJ9)
- **Slides:** [Phase 2 Presentation](https://docs.google.com/presentation/d/1Xad4EMYtHJDbGBUnD0eceDAAJnp_ZuNxo4-JTpAIwfg/edit?usp=sharing))
- **Streamlit:** Screenshots available in Appendix_3 (deployment pending)

## Data Sources

- **MBTA Developer Portal:** https://www.mbta.com/developers
- **Bluebikes System Data:** https://bluebikes.com/system-data

## License
Academic project for BA882 - Fall 2025

---

*For questions or issues, please contact the team via GitHub Issues.*
