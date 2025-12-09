# dags/policy_fetch_dag.py
from datetime import datetime
import re
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

from google.cloud import storage

# ============ CONFIG ============
PROJECT_ID = "ba882-f25-class-project-team9"

GCS_BUCKET = "transit-policy-scraped-data" 

POLICY_URLS = {
    "bluebikes_terms": "https://assets.bluebikes.com/rental-agreement.html",
    "bluebikes_usage": "https://bluebikes.com/how-it-works",
    "bluebikes_pricing": "https://bluebikes.com/pricing",
    "mbta_pricing": "https://www.mbta.com/fares", 
    "mbta_terms": "https://www.mbta.com/policies", 
}



def strip_html(raw_html: str) -> str:
    text = re.sub(r"<script.*?</script>", " ", raw_html, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def fetch_policies_to_gcs(**context):
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET)

    for name, url in POLICY_URLS.items():
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        cleaned_text = strip_html(resp.text)

        blob = bucket.blob(f"policies/{name}_raw.txt")
        blob.upload_from_string(cleaned_text, content_type="text/plain")


with DAG(
    dag_id="policy_fetch_dag",
    start_date=datetime(2025, 12, 1),
    schedule = None, 
    catchup=False,
    default_args={"retries": 1},
) as dag:

    fetch_policies_task = PythonOperator(
        task_id="fetch_policies_to_gcs",
        python_callable=fetch_policies_to_gcs,
    )



