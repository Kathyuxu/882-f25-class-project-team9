from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ================= CONFIG =================
PROJECT_ID = "ba882-f25-class-project-team9"

DATASET_RAW = "bluebikes_historical_us"        
TABLE_TRIPS = "JantoSep_historical"

DATASET_ANALYTICS = "bluebikes_analysis"      
DATASET_MODELS = "bluebikes_models"            
LOCATION = "US"

GCP_CONN_ID = "google_cloud_default"

STATION_INFO_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.station_info`"

START_TIME_COL = "started_at"
END_TIME_COL = "ended_at"
START_STATION_COL = "start_station_id"
END_STATION_COL = "end_station_id"

# ================= TABLE NAMES =================
TRIPS_FQN = f"`{PROJECT_ID}.{DATASET_RAW}.{TABLE_TRIPS}`"

GOLDEN_PICKUPS_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.golden_station_hourly_pickups`"
GOLDEN_DROPOFFS_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.golden_station_hourly_dropoffs`"

COMBINED_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.station_hourly_demand_combined`"
FEATURES_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.station_hourly_features`"

MODEL_OUTPUTS_PICKUPS_LR_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.model_outputs_pickups_lr`"
MODEL_OUTPUTS_PICKUPS_RF_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.model_outputs_pickups_rf`"
MODEL_OUTPUTS_PICKUPS_BT_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.model_outputs_pickups_bt`"

MODEL_OUTPUTS_DROPOFFS_LR_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.model_outputs_dropoffs_lr`"
MODEL_OUTPUTS_DROPOFFS_RF_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.model_outputs_dropoffs_rf`"
MODEL_OUTPUTS_DROPOFFS_BT_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.model_outputs_dropoffs_bt`"

MODEL_SELECTION_PICKUPS_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.model_selection_pickups`"
MODEL_SELECTION_DROPOFFS_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.model_selection_dropoffs`"

PREDICTED_PICKUPS_TBL  = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.predicted_pickups`"
PREDICTED_DROPOFFS_TBL = f"`{PROJECT_ID}.{DATASET_ANALYTICS}.predicted_dropoffs`"

PICKUPS_MODEL_LINREG = f"`{PROJECT_ID}.{DATASET_MODELS}.pickups_linreg`"
PICKUPS_MODEL_RF = f"`{PROJECT_ID}.{DATASET_MODELS}.pickups_random_forest`"
PICKUPS_MODEL_BT = f"`{PROJECT_ID}.{DATASET_MODELS}.pickups_boosted_tree`"
PICKUPS_MODEL_ARIMA = f"`{PROJECT_ID}.{DATASET_MODELS}.pickups_arima`"

DROPOFFS_MODEL_LINREG = f"`{PROJECT_ID}.{DATASET_MODELS}.dropoffs_linreg`"
DROPOFFS_MODEL_RF = f"`{PROJECT_ID}.{DATASET_MODELS}.dropoffs_random_forest`"
DROPOFFS_MODEL_BT = f"`{PROJECT_ID}.{DATASET_MODELS}.dropoffs_boosted_tree`"
DROPOFFS_MODEL_ARIMA = f"`{PROJECT_ID}.{DATASET_MODELS}.dropoffs_arima`"

# ================= SQL SCRIPTS =================

SQL_CREATE_SCHEMAS = f"""
CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET_ANALYTICS}`
OPTIONS(location="{LOCATION}");

CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET_MODELS}`
OPTIONS(location="{LOCATION}");
"""

SQL_ASSERT_SOURCE = f"""
DECLARE ok BOOL DEFAULT FALSE;

SET ok = EXISTS(
  SELECT 1
  FROM `{PROJECT_ID}.{DATASET_RAW}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{TABLE_TRIPS}'
);

IF NOT ok THEN
  RAISE USING MESSAGE = 'Missing source table: {PROJECT_ID}.{DATASET_RAW}.{TABLE_TRIPS}';
END IF;
"""

SQL_GOLDEN_PICKUPS = f"""
CREATE OR REPLACE TABLE {GOLDEN_PICKUPS_TBL} AS
WITH hourly AS (
  SELECT
    {START_STATION_COL} AS station_id,
    TIMESTAMP_TRUNC({START_TIME_COL}, HOUR) AS ts_hour,
    COUNT(*) AS pickups
  FROM {TRIPS_FQN}
  WHERE {START_STATION_COL} IS NOT NULL
  GROUP BY station_id, ts_hour
)
SELECT
  CAST(h.station_id AS STRING) AS station_id,
  si.name AS station_name,  
  h.ts_hour,
  EXTRACT(DAYOFWEEK FROM h.ts_hour) AS day_of_week,
  EXTRACT(HOUR      FROM h.ts_hour) AS hour,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM h.ts_hour) IN (1, 7) THEN 1
    ELSE 0
  END AS is_weekend,
  h.pickups
FROM hourly h
LEFT JOIN {STATION_INFO_TBL} si
  ON CAST(h.station_id AS STRING) = si.station_id;
"""

SQL_GOLDEN_DROPOFFS = f"""
CREATE OR REPLACE TABLE {GOLDEN_DROPOFFS_TBL} AS
WITH hourly AS (
  SELECT
    {END_STATION_COL} AS station_id,
    TIMESTAMP_TRUNC({END_TIME_COL}, HOUR) AS ts_hour,
    COUNT(*) AS dropoffs
  FROM {TRIPS_FQN}
  WHERE {END_STATION_COL} IS NOT NULL
  GROUP BY station_id, ts_hour
)
SELECT
  CAST(h.station_id AS STRING) AS station_id,
  si.name AS station_name,  
  h.ts_hour,
  EXTRACT(DAYOFWEEK FROM h.ts_hour) AS day_of_week,
  EXTRACT(HOUR      FROM h.ts_hour) AS hour,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM h.ts_hour) IN (1, 7) THEN 1
    ELSE 0
  END AS is_weekend,
  h.dropoffs
FROM hourly h
LEFT JOIN {STATION_INFO_TBL} si
  ON CAST(h.station_id AS STRING) = si.station_id;
"""

SQL_COMBINED = f"""
CREATE OR REPLACE TABLE {COMBINED_TBL} AS
SELECT
  COALESCE(p.station_id,   d.station_id)   AS station_id,
  COALESCE(p.station_name, d.station_name) AS station_name,
  COALESCE(p.ts_hour,      d.ts_hour)      AS ts_hour,
  COALESCE(p.day_of_week,  d.day_of_week)  AS day_of_week,
  COALESCE(p.hour,         d.hour)         AS hour,
  COALESCE(p.is_weekend,   d.is_weekend)   AS is_weekend,
  IFNULL(p.pickups,  0) AS pickups,
  IFNULL(d.dropoffs, 0) AS dropoffs
FROM {GOLDEN_PICKUPS_TBL} p
FULL JOIN {GOLDEN_DROPOFFS_TBL} d
USING (station_id, ts_hour);
"""

SQL_FEATURES = f"""
CREATE OR REPLACE TABLE {FEATURES_TBL} AS
SELECT
  station_id,
  station_name,
  ts_hour,
  day_of_week,
  hour,
  is_weekend,
  pickups,
  dropoffs
FROM {COMBINED_TBL};
"""

SQL_INIT_OUTPUT_TABLES = f"""
CREATE OR REPLACE TABLE {MODEL_OUTPUTS_PICKUPS_LR_TBL} (
  ts_hour TIMESTAMP,
  station_id STRING,
  y_true FLOAT64,
  y_pred FLOAT64
);

CREATE OR REPLACE TABLE {MODEL_OUTPUTS_PICKUPS_RF_TBL} (
  ts_hour TIMESTAMP,
  station_id STRING,
  y_true FLOAT64,
  y_pred FLOAT64
);

CREATE OR REPLACE TABLE {MODEL_OUTPUTS_PICKUPS_BT_TBL} (
  ts_hour TIMESTAMP,
  station_id STRING,
  y_true FLOAT64,
  y_pred FLOAT64
);

CREATE OR REPLACE TABLE {MODEL_OUTPUTS_DROPOFFS_LR_TBL} (
  ts_hour TIMESTAMP,
  station_id STRING,
  y_true FLOAT64,
  y_pred FLOAT64
);

CREATE OR REPLACE TABLE {MODEL_OUTPUTS_DROPOFFS_RF_TBL} (
  ts_hour TIMESTAMP,
  station_id STRING,
  y_true FLOAT64,
  y_pred FLOAT64
);

CREATE OR REPLACE TABLE {MODEL_OUTPUTS_DROPOFFS_BT_TBL} (
  ts_hour TIMESTAMP,
  station_id STRING,
  y_true FLOAT64,
  y_pred FLOAT64
);
"""

# ---------- BQML: TRAIN PICKUPS ----------
SQL_TRAIN_PICKUPS_LINREG = f"""
CREATE OR REPLACE MODEL {PICKUPS_MODEL_LINREG}
OPTIONS(
  model_type = 'linear_reg',
  input_label_cols = ['pickups']
) AS
SELECT
  CAST(station_id AS STRING) AS station_id,
  day_of_week,
  hour,
  is_weekend,
  pickups
FROM {FEATURES_TBL};
"""

SQL_TRAIN_PICKUPS_RF = f"""
CREATE OR REPLACE MODEL {PICKUPS_MODEL_RF}
OPTIONS(
  model_type = 'random_forest_regressor',
  input_label_cols = ['pickups']
) AS
SELECT
  CAST(station_id AS STRING) AS station_id,
  day_of_week,
  hour,
  is_weekend,
  pickups
FROM {FEATURES_TBL};
"""

SQL_TRAIN_PICKUPS_BT = f"""
CREATE OR REPLACE MODEL {PICKUPS_MODEL_BT}
OPTIONS(
  model_type = 'boosted_tree_regressor',
  input_label_cols = ['pickups'],
  max_iterations = 30
) AS
SELECT
  CAST(station_id AS STRING) AS station_id,
  day_of_week,
  hour,
  is_weekend,
  pickups
FROM {FEATURES_TBL};
"""

SQL_TRAIN_PICKUPS_ARIMA = f"""
CREATE OR REPLACE MODEL {PICKUPS_MODEL_ARIMA}
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'ts_hour',
  time_series_data_col = 'pickups',
  time_series_id_col = 'station_id'
) AS
SELECT
  ts_hour,
  pickups,
  station_id
FROM {FEATURES_TBL};
"""

# ---------- BQML: TRAIN DROPOFFS ----------
SQL_TRAIN_DROPOFFS_LINREG = f"""
CREATE OR REPLACE MODEL {DROPOFFS_MODEL_LINREG}
OPTIONS(
  model_type = 'linear_reg',
  input_label_cols = ['dropoffs']
) AS
SELECT
  CAST(station_id AS STRING) AS station_id,
  day_of_week,
  hour,
  is_weekend,
  dropoffs
FROM {FEATURES_TBL};
"""

SQL_TRAIN_DROPOFFS_RF = f"""
CREATE OR REPLACE MODEL {DROPOFFS_MODEL_RF}
OPTIONS(
  model_type = 'random_forest_regressor',
  input_label_cols = ['dropoffs']
) AS
SELECT
  CAST(station_id AS STRING) AS station_id,
  day_of_week,
  hour,
  is_weekend,
  dropoffs
FROM {FEATURES_TBL};
"""

SQL_TRAIN_DROPOFFS_BT = f"""
CREATE OR REPLACE MODEL {DROPOFFS_MODEL_BT}
OPTIONS(
  model_type = 'boosted_tree_regressor',
  input_label_cols = ['dropoffs'],
  max_iterations = 30
) AS
SELECT
  CAST(station_id AS STRING) AS station_id,
  day_of_week,
  hour,
  is_weekend,
  dropoffs
FROM {FEATURES_TBL};
"""

SQL_TRAIN_DROPOFFS_ARIMA = f"""
CREATE OR REPLACE MODEL {DROPOFFS_MODEL_ARIMA}
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'ts_hour',
  time_series_data_col = 'dropoffs',
  time_series_id_col = 'station_id'
) AS
SELECT
  ts_hour,
  dropoffs,
  station_id
FROM {FEATURES_TBL};
"""

_SQL_FEATURE_PRED_PICKUPS = f"""
SELECT
  CAST(station_id AS STRING) AS station_id,
  day_of_week,
  hour,
  is_weekend,
  pickups
FROM {FEATURES_TBL}
"""

_SQL_FEATURE_PRED_DROPOFFS = f"""
SELECT
  CAST(station_id AS STRING) AS station_id,
  day_of_week,
  hour,
  is_weekend,
  dropoffs
FROM {FEATURES_TBL}
"""

SQL_PRED_PICKUPS_LINREG = f"""
CREATE OR REPLACE TABLE {MODEL_OUTPUTS_PICKUPS_LR_TBL} AS
WITH preds AS (
  SELECT * FROM ML.PREDICT(
    MODEL {PICKUPS_MODEL_LINREG},
    ({_SQL_FEATURE_PRED_PICKUPS})
  )
)
SELECT
  f.ts_hour,
  f.station_id,
  f.pickups AS y_true,
  p.predicted_pickups AS y_pred
FROM preds p
JOIN {FEATURES_TBL} f
ON CAST(f.station_id AS STRING) = p.station_id
AND f.day_of_week = p.day_of_week
AND f.hour = p.hour
AND f.is_weekend = p.is_weekend;
"""

SQL_PRED_PICKUPS_RF = f"""
CREATE OR REPLACE TABLE {MODEL_OUTPUTS_PICKUPS_RF_TBL} AS
WITH preds AS (
  SELECT * FROM ML.PREDICT(
    MODEL {PICKUPS_MODEL_RF},
    ({_SQL_FEATURE_PRED_PICKUPS})
  )
)
SELECT
  f.ts_hour,
  f.station_id,
  f.pickups AS y_true,
  p.predicted_pickups AS y_pred
FROM preds p
JOIN {FEATURES_TBL} f
ON CAST(f.station_id AS STRING) = p.station_id
AND f.day_of_week = p.day_of_week
AND f.hour = p.hour
AND f.is_weekend = p.is_weekend;
"""

SQL_PRED_PICKUPS_BT = f"""
CREATE OR REPLACE TABLE {MODEL_OUTPUTS_PICKUPS_BT_TBL} AS
WITH preds AS (
  SELECT * FROM ML.PREDICT(
    MODEL {PICKUPS_MODEL_BT},
    ({_SQL_FEATURE_PRED_PICKUPS})
  )
)
SELECT
  f.ts_hour,
  f.station_id,
  f.pickups AS y_true,
  p.predicted_pickups AS y_pred
FROM preds p
JOIN {FEATURES_TBL} f
ON CAST(f.station_id AS STRING) = p.station_id
AND f.day_of_week = p.day_of_week
AND f.hour = p.hour
AND f.is_weekend = p.is_weekend;
"""

SQL_PRED_DROPOFFS_LINREG = f"""
CREATE OR REPLACE TABLE {MODEL_OUTPUTS_DROPOFFS_LR_TBL} AS
WITH preds AS (
  SELECT * FROM ML.PREDICT(
    MODEL {DROPOFFS_MODEL_LINREG},
    ({_SQL_FEATURE_PRED_DROPOFFS})
  )
)
SELECT
  f.ts_hour,
  f.station_id,
  f.dropoffs AS y_true,
  p.predicted_dropoffs AS y_pred
FROM preds p
JOIN {FEATURES_TBL} f
ON CAST(f.station_id AS STRING) = p.station_id
AND f.day_of_week = p.day_of_week
AND f.hour = p.hour
AND f.is_weekend = p.is_weekend;
"""

SQL_PRED_DROPOFFS_RF = f"""
CREATE OR REPLACE TABLE {MODEL_OUTPUTS_DROPOFFS_RF_TBL} AS
WITH preds AS (
  SELECT * FROM ML.PREDICT(
    MODEL {DROPOFFS_MODEL_RF},
    ({_SQL_FEATURE_PRED_DROPOFFS})
  )
)
SELECT
  f.ts_hour,
  f.station_id,
  f.dropoffs AS y_true,
  p.predicted_dropoffs AS y_pred
FROM preds p
JOIN {FEATURES_TBL} f
ON CAST(f.station_id AS STRING) = p.station_id
AND f.day_of_week = p.day_of_week
AND f.hour = p.hour
AND f.is_weekend = p.is_weekend;
"""

SQL_PRED_DROPOFFS_BT = f"""
CREATE OR REPLACE TABLE {MODEL_OUTPUTS_DROPOFFS_BT_TBL} AS
WITH preds AS (
  SELECT * FROM ML.PREDICT(
    MODEL {DROPOFFS_MODEL_BT},
    ({_SQL_FEATURE_PRED_DROPOFFS})
  )
)
SELECT
  f.ts_hour,
  f.station_id,
  f.dropoffs AS y_true,
  p.predicted_dropoffs AS y_pred
FROM preds p
JOIN {FEATURES_TBL} f
ON CAST(f.station_id AS STRING) = p.station_id
AND f.day_of_week = p.day_of_week
AND f.hour = p.hour
AND f.is_weekend = p.is_weekend;
"""

SQL_FIND_BEST_PICKUPS = f"""
CREATE OR REPLACE TABLE {MODEL_SELECTION_PICKUPS_TBL} AS
WITH all_metrics AS (
  SELECT 'pickups_linreg' AS model_name,
         SQRT(AVG(POW(y_true - y_pred, 2))) AS rmse
  FROM {MODEL_OUTPUTS_PICKUPS_LR_TBL}

  UNION ALL
  SELECT 'pickups_random_forest' AS model_name,
         SQRT(AVG(POW(y_true - y_pred, 2))) AS rmse
  FROM {MODEL_OUTPUTS_PICKUPS_RF_TBL}

  UNION ALL
  SELECT 'pickups_boosted_tree' AS model_name,
         SQRT(AVG(POW(y_true - y_pred, 2))) AS rmse
  FROM {MODEL_OUTPUTS_PICKUPS_BT_TBL}

  UNION ALL
  SELECT 'pickups_arima' AS model_name,
         SQRT(AVG(variance)) AS rmse
  FROM ML.ARIMA_EVALUATE(MODEL {PICKUPS_MODEL_ARIMA})
)
SELECT *
FROM all_metrics
QUALIFY rmse = MIN(rmse) OVER();
"""

SQL_FIND_BEST_DROPOFFS = f"""
CREATE OR REPLACE TABLE {MODEL_SELECTION_DROPOFFS_TBL} AS
WITH all_metrics AS (
  SELECT 'dropoffs_linreg' AS model_name,
         SQRT(AVG(POW(y_true - y_pred, 2))) AS rmse
  FROM {MODEL_OUTPUTS_DROPOFFS_LR_TBL}

  UNION ALL
  SELECT 'dropoffs_random_forest' AS model_name,
         SQRT(AVG(POW(y_true - y_pred, 2))) AS rmse
  FROM {MODEL_OUTPUTS_DROPOFFS_RF_TBL}

  UNION ALL
  SELECT 'dropoffs_boosted_tree' AS model_name,
         SQRT(AVG(POW(y_true - y_pred, 2))) AS rmse
  FROM {MODEL_OUTPUTS_DROPOFFS_BT_TBL}

  UNION ALL
  SELECT 'dropoffs_arima' AS model_name,
         SQRT(AVG(variance)) AS rmse
  FROM ML.ARIMA_EVALUATE(MODEL {DROPOFFS_MODEL_ARIMA})
)
SELECT *
FROM all_metrics
QUALIFY rmse = MIN(rmse) OVER();
"""

SQL_FINAL_PRED_PICKUPS = f"""
CREATE OR REPLACE TABLE {PREDICTED_PICKUPS_TBL} AS
WITH base AS (
  SELECT
    f.station_id,
    f.station_name,
    EXTRACT(DAYOFWEEK FROM f.ts_hour) AS day_of_week,
    EXTRACT(HOUR      FROM f.ts_hour) AS hour,
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM f.ts_hour) IN (1,7) THEN 1
      ELSE 0
    END AS is_weekend,
    o.y_pred AS predicted_pickups
  FROM {MODEL_OUTPUTS_PICKUPS_BT_TBL} o
  JOIN {FEATURES_TBL} f
    ON f.ts_hour = o.ts_hour
   AND f.station_id = o.station_id
)
SELECT
  station_id,
  ANY_VALUE(station_name) AS station_name,
  day_of_week,
  hour,
  is_weekend,
  AVG(predicted_pickups) AS predicted_pickups
FROM base
GROUP BY station_id, day_of_week, hour, is_weekend;
"""

SQL_FINAL_PRED_DROPOFFS = f"""
CREATE OR REPLACE TABLE {PREDICTED_DROPOFFS_TBL} AS
WITH base AS (
  SELECT
    f.station_id,
    f.station_name,
    EXTRACT(DAYOFWEEK FROM f.ts_hour) AS day_of_week,
    EXTRACT(HOUR      FROM f.ts_hour) AS hour,
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM f.ts_hour) IN (1,7) THEN 1
      ELSE 0
    END AS is_weekend,
    o.y_pred AS predicted_dropoffs
  FROM {MODEL_OUTPUTS_DROPOFFS_BT_TBL} o
  JOIN {FEATURES_TBL} f
    ON f.ts_hour = o.ts_hour
   AND f.station_id = o.station_id
)
SELECT
  station_id,
  ANY_VALUE(station_name) AS station_name,
  day_of_week,
  hour,
  is_weekend,
  AVG(predicted_dropoffs) AS predicted_dropoffs
FROM base
GROUP BY station_id, day_of_week, hour, is_weekend;
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bluebikes_station_hour_demand",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1, 
    tags=["ba882", "bluebikes"],
) as dag:

    create_schemas = BigQueryInsertJobOperator(
        task_id="create_schemas",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_CREATE_SCHEMAS, "useLegacySql": False}},
    )

    assert_source = BigQueryInsertJobOperator(
        task_id="assert_source",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_ASSERT_SOURCE, "useLegacySql": False}},
    )

    golden_pickups = BigQueryInsertJobOperator(
        task_id="golden_pickups",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_GOLDEN_PICKUPS, "useLegacySql": False}},
    )

    golden_dropoffs = BigQueryInsertJobOperator(
        task_id="golden_dropoffs",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_GOLDEN_DROPOFFS, "useLegacySql": False}},
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

    init_outputs = BigQueryInsertJobOperator(
        task_id="init_outputs",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_INIT_OUTPUT_TABLES, "useLegacySql": False}},
    )

    # --- pickups models ---
    train_pickups_linreg = BigQueryInsertJobOperator(
        task_id="train_pickups_linreg",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_TRAIN_PICKUPS_LINREG, "useLegacySql": False}},
    )

    train_pickups_rf = BigQueryInsertJobOperator(
        task_id="train_pickups_rf",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_TRAIN_PICKUPS_RF, "useLegacySql": False}},
    )

    train_pickups_bt = BigQueryInsertJobOperator(
        task_id="train_pickups_bt",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_TRAIN_PICKUPS_BT, "useLegacySql": False}},
    )

    train_pickups_arima = BigQueryInsertJobOperator(
        task_id="train_pickups_arima",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_TRAIN_PICKUPS_ARIMA, "useLegacySql": False}},
    )

    pred_pickups_linreg = BigQueryInsertJobOperator(
        task_id="pred_pickups_linreg",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_PRED_PICKUPS_LINREG, "useLegacySql": False}},
    )

    pred_pickups_rf = BigQueryInsertJobOperator(
        task_id="pred_pickups_rf",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_PRED_PICKUPS_RF, "useLegacySql": False}},
    )

    pred_pickups_bt = BigQueryInsertJobOperator(
        task_id="pred_pickups_bt",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_PRED_PICKUPS_BT, "useLegacySql": False}},
    )

    find_best_pickups = BigQueryInsertJobOperator(
        task_id="find_best_pickups",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_FIND_BEST_PICKUPS, "useLegacySql": False}},
    )

    # --- dropoffs models ---
    train_dropoffs_linreg = BigQueryInsertJobOperator(
        task_id="train_dropoffs_linreg",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_TRAIN_DROPOFFS_LINREG, "useLegacySql": False}},
    )

    train_dropoffs_rf = BigQueryInsertJobOperator(
        task_id="train_dropoffs_rf",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_TRAIN_DROPOFFS_RF, "useLegacySql": False}},
    )

    train_dropoffs_bt = BigQueryInsertJobOperator(
        task_id="train_dropoffs_bt",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_TRAIN_DROPOFFS_BT, "useLegacySql": False}},
    )

    train_dropoffs_arima = BigQueryInsertJobOperator(
        task_id="train_dropoffs_arima",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_TRAIN_DROPOFFS_ARIMA, "useLegacySql": False}},
    )

    pred_dropoffs_linreg = BigQueryInsertJobOperator(
        task_id="pred_dropoffs_linreg",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_PRED_DROPOFFS_LINREG, "useLegacySql": False}},
    )

    pred_dropoffs_rf = BigQueryInsertJobOperator(
        task_id="pred_dropoffs_rf",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_PRED_DROPOFFS_RF, "useLegacySql": False}},
    )

    pred_dropoffs_bt = BigQueryInsertJobOperator(
        task_id="pred_dropoffs_bt",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_PRED_DROPOFFS_BT, "useLegacySql": False}},
    )

    find_best_dropoffs = BigQueryInsertJobOperator(
        task_id="find_best_dropoffs",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_FIND_BEST_DROPOFFS, "useLegacySql": False}},
    )

    final_pred_pickups = BigQueryInsertJobOperator(
        task_id="final_pred_pickups",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_FINAL_PRED_PICKUPS, "useLegacySql": False}},
    )

    final_pred_dropoffs = BigQueryInsertJobOperator(
        task_id="final_pred_dropoffs",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={"query": {"query": SQL_FINAL_PRED_DROPOFFS, "useLegacySql": False}},
    )

    create_schemas >> assert_source >> [golden_pickups, golden_dropoffs]
    [golden_pickups, golden_dropoffs] >> make_combined >> make_features >> init_outputs

    init_outputs >> [
        train_pickups_linreg,
        train_pickups_rf,
        train_pickups_bt,
        train_pickups_arima,
        train_dropoffs_linreg,
        train_dropoffs_rf,
        train_dropoffs_bt,
        train_dropoffs_arima,
    ]

    train_pickups_linreg >> pred_pickups_linreg
    train_pickups_rf >> pred_pickups_rf
    train_pickups_bt >> pred_pickups_bt

    [pred_pickups_linreg, pred_pickups_rf, pred_pickups_bt, train_pickups_arima] >> find_best_pickups

    train_dropoffs_linreg >> pred_dropoffs_linreg
    train_dropoffs_rf >> pred_dropoffs_rf
    train_dropoffs_bt >> pred_dropoffs_bt

    [pred_dropoffs_linreg, pred_dropoffs_rf, pred_dropoffs_bt, train_dropoffs_arima] >> find_best_dropoffs

    pred_pickups_bt >> final_pred_pickups
    pred_dropoffs_bt >> final_pred_dropoffs
