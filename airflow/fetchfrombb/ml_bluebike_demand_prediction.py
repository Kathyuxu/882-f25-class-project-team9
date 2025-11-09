# dags/bluebikes_station_hour_demand.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ================= CONFIG =================
PROJECT_ID = "ba882-f25-class-project-team9"

DATASET_RAW = "bluebikes_historical_us"        # raw dataset (region: US)
TABLE_TRIPS = "JantoSep_historical"

DATASET_ANALYTICS = "bluebikes_analysis"       # outputs (same region)
LOCATION = "US"

GCP_CONN_ID = "google_cloud_default"

# =============== FQNs / TABLES ===============
TRIPS_FQN = f"`{PROJECT_ID}.{DATASET_RAW}.{TABLE_TRIPS}`"

PICKUPS_TBL   = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.station_hourly_pickups`"
DROPOFFS_TBL  = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.station_hourly_dropoffs`"
COMBINED_TBL  = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.station_hourly_demand_combined`"
FEATURES_TBL  = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.station_hourly_features`"

PICKUP_MODEL  = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.pickup_demand_linreg`"
DROPOFF_MODEL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.dropoff_demand_linreg`"

EVAL_PICKUPS_TBL  = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.eval_pickups_linreg`"
EVAL_DROPOFFS_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.eval_dropoffs_linreg`"

PRED_PICKUPS_TBL  = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.predicted_pickups`"
PRED_DROPOFFS_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.predicted_dropoffs`"

# =============== SQL GUARDRAILS ===============
SQL_CREATE_ANALYTICS = f"""
CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET_ANALYTICS}`
OPTIONS(location='{LOCATION}');
"""

SQL_ASSERT_SOURCE = f"""
DECLARE ok BOOL;
SET ok = EXISTS(
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_RAW}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{TABLE_TRIPS}'
);
IF NOT ok THEN
  RAISE USING MESSAGE = 'Missing source table: {PROJECT_ID}.{DATASET_RAW}.{TABLE_TRIPS}';
END IF;
"""

# =============== AGG + FEATURES ===============
SQL_AGG_PICKUPS = f"""
CREATE OR REPLACE TABLE {PICKUPS_TBL} AS
SELECT
  start_station_id AS station_id,
  EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,
  EXTRACT(HOUR FROM started_at) AS hour,
  COUNT(*) AS pickups
FROM {TRIPS_FQN}
WHERE start_station_id IS NOT NULL
GROUP BY station_id, day_of_week, hour
"""

SQL_AGG_DROPOFFS = f"""
CREATE OR REPLACE TABLE {DROPOFFS_TBL} AS
SELECT
  end_station_id AS station_id,
  EXTRACT(DAYOFWEEK FROM ended_at) AS day_of_week,
  EXTRACT(HOUR FROM ended_at) AS hour,
  COUNT(*) AS dropoffs
FROM {TRIPS_FQN}
WHERE end_station_id IS NOT NULL
GROUP BY station_id, day_of_week, hour
"""

SQL_COMBINED = f"""
CREATE OR REPLACE TABLE {COMBINED_TBL} AS
SELECT
  COALESCE(p.station_id, d.station_id) AS station_id,
  COALESCE(p.day_of_week, d.day_of_week) AS day_of_week,
  COALESCE(p.hour, d.hour) AS hour,
  IFNULL(p.pickups, 0) AS pickups,
  IFNULL(d.dropoffs, 0) AS dropoffs
FROM {PICKUPS_TBL} p
FULL JOIN {DROPOFFS_TBL} d
USING (station_id, day_of_week, hour)
"""

SQL_FEATURES = f"""
CREATE OR REPLACE TABLE {FEATURES_TBL} AS
SELECT
  CAST(station_id AS STRING) AS station_id,
  day_of_week,
  hour,
  CASE WHEN day_of_week IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
  pickups,
  dropoffs
FROM {COMBINED_TBL}
"""

# =============== TRAIN + EVAL (BQML) ===============
SQL_TRAIN_PICKUPS = f"""
CREATE OR REPLACE MODEL {PICKUP_MODEL}
OPTIONS(
  model_type='linear_reg',
  input_label_cols=['pickups'],
  data_split_method='RANDOM',
  data_split_eval_fraction=0.2
) AS
SELECT station_id, day_of_week, hour, is_weekend, pickups
FROM {FEATURES_TBL}
"""

SQL_TRAIN_DROPOFFS = f"""
CREATE OR REPLACE MODEL {DROPOFF_MODEL}
OPTIONS(
  model_type='linear_reg',
  input_label_cols=['dropoffs'],
  data_split_method='RANDOM',
  data_split_eval_fraction=0.2
) AS
SELECT station_id, day_of_week, hour, is_weekend, dropoffs
FROM {FEATURES_TBL}
"""

SQL_EVAL_PICKUPS = f"""
CREATE OR REPLACE TABLE {EVAL_PICKUPS_TBL} AS
SELECT * FROM ML.EVALUATE(MODEL {PICKUP_MODEL})
"""

SQL_EVAL_DROPOFFS = f"""
CREATE OR REPLACE TABLE {EVAL_DROPOFFS_TBL} AS
SELECT * FROM ML.EVALUATE(MODEL {DROPOFF_MODEL})
"""

# =============== PREDICT (WITH NAMES) ===============
SQL_PRED_PICKUPS = f"""
CREATE OR REPLACE TABLE {PRED_PICKUPS_TBL} AS
WITH base AS (
  SELECT
    CAST(station_id AS STRING) AS station_id,
    day_of_week,
    hour,
    is_weekend,
    predicted_pickups
  FROM ML.PREDICT(MODEL {PICKUP_MODEL},
       (SELECT station_id, day_of_week, hour, is_weekend FROM {FEATURES_TBL}))
),
names AS (
  SELECT
    CAST(start_station_id AS STRING) AS station_id,
    ANY_VALUE(start_station_name) AS station_name
  FROM {TRIPS_FQN}
  WHERE start_station_id IS NOT NULL AND start_station_name IS NOT NULL
  GROUP BY station_id
)
SELECT
  b.station_id,
  n.station_name,
  b.day_of_week,
  b.hour,
  b.is_weekend,
  b.predicted_pickups
FROM base b
LEFT JOIN names n USING (station_id)
"""

SQL_PRED_DROPOFFS = f"""
CREATE OR REPLACE TABLE {PRED_DROPOFFS_TBL} AS
WITH base AS (
  SELECT
    CAST(station_id AS STRING) AS station_id,
    day_of_week,
    hour,
    is_weekend,
    predicted_dropoffs
  FROM ML.PREDICT(MODEL {DROPOFF_MODEL},
       (SELECT station_id, day_of_week, hour, is_weekend FROM {FEATURES_TBL}))
),
names AS (
  SELECT
    CAST(end_station_id AS STRING) AS station_id,
    ANY_VALUE(end_station_name) AS station_name
  FROM {TRIPS_FQN}
  WHERE end_station_id IS NOT NULL AND end_station_name IS NOT NULL
  GROUP BY station_id
)
SELECT
  b.station_id,
  n.station_name,
  b.day_of_week,
  b.hour,
  b.is_weekend,
  b.predicted_dropoffs
FROM base b
LEFT JOIN names n USING (station_id)
"""

# =============== DAG =================
default_args = {
    "owner": "ba882-team9",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bluebikes_station_hour_linreg",
    start_date=datetime(2025, 11, 1),
    schedule="@daily",   # <-- compatible with older Airflow
    catchup=False,
    default_args=default_args,
    tags=["bluebikes", "bqml", "mlops"],
) as dag:

    create_analytics = BigQueryInsertJobOperator(
        task_id="create_analytics_dataset",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_CREATE_ANALYTICS, "useLegacySql": False}},
    )

    assert_source = BigQueryInsertJobOperator(
        task_id="assert_source_table",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_ASSERT_SOURCE, "useLegacySql": False}},
    )

    agg_pickups = BigQueryInsertJobOperator(
        task_id="agg_pickups",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_AGG_PICKUPS, "useLegacySql": False}},
    )

    agg_dropoffs = BigQueryInsertJobOperator(
        task_id="agg_dropoffs",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_AGG_DROPOFFS, "useLegacySql": False}},
    )

    make_combined = BigQueryInsertJobOperator(
        task_id="make_combined",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_COMBINED, "useLegacySql": False}},
    )

    make_features = BigQueryInsertJobOperator(
        task_id="make_features",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_FEATURES, "useLegacySql": False}},
    )

    train_pickups = BigQueryInsertJobOperator(
        task_id="train_pickups_linreg",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_TRAIN_PICKUPS, "useLegacySql": False}},
    )

    train_dropoffs = BigQueryInsertJobOperator(
        task_id="train_dropoffs_linreg",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_TRAIN_DROPOFFS, "useLegacySql": False}},
    )

    eval_pickups = BigQueryInsertJobOperator(
        task_id="evaluate_pickups",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_EVAL_PICKUPS, "useLegacySql": False}},
    )

    eval_dropoffs = BigQueryInsertJobOperator(
        task_id="evaluate_dropoffs",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_EVAL_DROPOFFS, "useLegacySql": False}},
    )

    predict_pickups = BigQueryInsertJobOperator(
        task_id="predict_pickups",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_PRED_PICKUPS, "useLegacySql": False}},
    )

    predict_dropoffs = BigQueryInsertJobOperator(
        task_id="predict_dropoffs",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_PRED_DROPOFFS, "useLegacySql": False}},
    )

    create_analytics >> assert_source >> [agg_pickups, agg_dropoffs] >> make_combined >> make_features
    make_features >> [train_pickups, train_dropoffs]
    train_pickups >> eval_pickups >> predict_pickups
    train_dropoffs >> eval_dropoffs >> predict_dropoffs
