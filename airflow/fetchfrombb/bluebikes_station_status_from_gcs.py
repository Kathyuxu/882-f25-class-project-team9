from __future__ import annotations
import re, io, json
import datetime as dt
from zoneinfo import ZoneInfo
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.exceptions import AirflowNotFoundException
from google.cloud import storage, bigquery

PROJECT_ID = "ba882-f25-class-project-team9"
DATASET = "bluebikes_analysis"
TABLE = "station_status"
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"
BQ_LOCATION = "US"

BUCKET = "bluebikes-raw--f25-class-project-team9"
PREFIX = "raw/bluebikes/station_status/"

RUN_RE = re.compile(r"run_(\d{8}T\d{6}Z)\.json$")
DT_RE  = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/")

BOSTON_TZ = ZoneInfo("America/New_York")


def pick_latest_blob(blobs):
    best = None
    best_key = None
    for b in blobs:
        m = RUN_RE.search(b.name)
        key = m.group(1) if m else (b.updated.isoformat() if hasattr(b, "updated") else "")
        if best_key is None or key > best_key:
            best, best_key = b, key
    return best


def get_gcp_clients():
    try:
        hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
        creds = hook.get_credentials()
        gs = storage.Client(project=PROJECT_ID, credentials=creds)
        bq = bigquery.Client(project=PROJECT_ID, credentials=creds, location=BQ_LOCATION)
        return gs, bq
    except AirflowNotFoundException:
        gs = storage.Client(project=PROJECT_ID)
        bq = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
        return gs, bq


def load_latest_and_replace(**_):
    gs, bq = get_gcp_clients()
    blobs = list(gs.list_blobs(BUCKET, prefix=PREFIX))
    if not blobs:
        raise RuntimeError(f"No blobs under gs://{BUCKET}/{PREFIX}")

    latest = pick_latest_blob(blobs)
    if latest is None:
        raise RuntimeError("Failed to pick latest blob")

    buf = io.BytesIO()
    latest.download_to_file(buf)
    buf.seek(0)
    payload = json.load(buf)

    stations = (payload.get("data") or {}).get("stations") or []
    if not stations:
        raise ValueError(f"Empty stations list in {latest.name}")

    df = pd.json_normalize(stations)

    
    lu_unix = payload.get("last_updated")
    if lu_unix is not None:
        ts_utc = pd.to_datetime(lu_unix, unit="s", utc=True)
        ts_boston = ts_utc.tz_convert(BOSTON_TZ)
        df["snapshot_ts"] = ts_boston.strftime("%Y-%m-%d %H:%M:%S %Z")
    else:
        df["snapshot_ts"] = pd.Timestamp.now(tz=BOSTON_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

    df["ingested_at"] = pd.Timestamp.now(tz=BOSTON_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

    if "last_reported" in df.columns:
        df["last_reported_ts"] = (
            pd.to_datetime(df["last_reported"], unit="s", utc=True)
              .dt.tz_convert(BOSTON_TZ)
              .dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        )
    

    print(f"Boston time now: {dt.datetime.now(BOSTON_TZ).isoformat()}")

    m_dt = DT_RE.search(latest.name)
    df["dt"] = m_dt.group(1) if m_dt else None
    m_run = RUN_RE.search(latest.name)
    df["run_id"] = m_run.group(1) if m_run else None
    df["gcs_object"] = f"gs://{BUCKET}/{latest.name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = bq.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()

    print(f"Replaced {TABLE_ID} with {len(df)} rows from {latest.name}")


with DAG(
    dag_id="bluebikes_station_status_from_gcs",
    start_date=dt.datetime(2025, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "team9", "retries": 2, "retry_delay": dt.timedelta(seconds=30)},
    tags=["bluebikes", "gbfs", "snapshot", "bigquery", "gcs"],
) as dag:
    replace_latest = PythonOperator(
        task_id="load_latest_and_replace",
        python_callable=load_latest_and_replace,
    )
