import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def _get_airflow_var(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Prefer environment overrides AIRFLOW_VAR_<KEY>, otherwise fall back to Airflow Variables.
    """
    env_key = f"AIRFLOW_VAR_{key.upper()}"
    if env_key in os.environ:
        return os.environ[env_key]
    return Variable.get(key, default_var=default)


GCP_CONN_ID = "google_cloud_default"
GCS_BUCKET = _get_airflow_var("GCS_BUCKET", "bluebikes-raw--f25-class-project-team9")
BASE_PREFIX = _get_airflow_var("BLUEBIKES_GCS_PREFIX", "raw/bluebikes")

BLUEBIKES_BASE = "https://gbfs.lyft.com/gbfs/1.1/bos/en"

GBFS_FEEDS = [
    {
        "name": "station_information",
        "endpoint": "station_information.json",
        "description": "Static station info (lat/lon, capacity)",
    },
    {
        "name": "station_status",
        "endpoint": "station_status.json",
        "description": "Real-time dock availability",
    },
    {
        "name": "system_regions",
        "endpoint": "system_regions.json",
        "description": "System regions",
    },
]


def _upload_to_gcs(feed_name: str, json_text: str, ds: str, **_) -> str:
    """
    Upload JSON string to GCS.
    Path: gs://<bucket>/<prefix>/<feed_name>/dt=<ds>/run_<timestamp>.json
    """
    gcs = GCSHook(gcp_conn_id=GCP_CONN_ID)
    
    now_utc = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    
    object_name = f"{BASE_PREFIX}/{feed_name}/dt={ds}/run_{now_utc}.json"
    
    gcs.upload(
        bucket_name=GCS_BUCKET,
        object_name=object_name,
        data=json_text,
        mime_type="application/json",
    )
    
    return f"gs://{GCS_BUCKET}/{object_name}"


def _fetch_and_upload(feed_name: str, endpoint: str, ds: str, **_) -> str:
    """
    Fetch from Bluebikes GBFS API and upload to GCS.
    Using requests directly since HttpOperator can be finicky with XCom.
    """
    import requests
    
    url = f"{BLUEBIKES_BASE}/{endpoint}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        json_text = response.text
        
        gcs_path = _upload_to_gcs(feed_name, json_text, ds)
        
        return gcs_path
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch {feed_name} from {url}: {str(e)}")


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="bluebikes_gbfs_to_gcs",
    description="Fetch Bluebikes GBFS feeds and upload to GCS",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="@hourly", 
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bluebikes", "gbfs", "gcs"],
) as dag:

    tasks = []
    for feed in GBFS_FEEDS:
        task = PythonOperator(
            task_id=f"fetch_{feed['name']}",
            python_callable=_fetch_and_upload,
            op_kwargs={
                "feed_name": feed["name"],
                "endpoint": feed["endpoint"],
            },
        )
        tasks.append(task)




