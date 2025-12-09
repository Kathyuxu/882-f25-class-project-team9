# dags/policy_process_dag.py
from datetime import datetime
import re

from airflow import DAG
from airflow.operators.python import PythonOperator

from google.cloud import storage, bigquery

PROJECT_ID = "ba882-f25-class-project-team9"
GCS_BUCKET = "transit-policy-scraped-data"

DATASET_LLM = "bluebikes_llm"
POLICY_CHUNKS_TABLE = f"{PROJECT_ID}.{DATASET_LLM}.policy_chunks"

POLICY_SOURCES = [
    "bluebikes_terms",
    "bluebikes_rules",
    "mbta_bike_policy",
]


def chunk_policies_to_bq(**context):
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET)
    bq_client = bigquery.Client(project=PROJECT_ID)

    rows = []

    for name in POLICY_SOURCES:
        blob = bucket.blob(f"policies/{name}_raw.txt")
        if not blob.exists():
            continue

        text = blob.download_as_text()
        sentences = re.split(r"(?<=[\.\?\!])\s+|\n+", text)

        current = []
        current_len = 0
        chunk_id = 0

        for sent in sentences:
            s = sent.strip()
            if not s:
                continue

            if current_len + len(s) > 800 and current:
                chunk_text = " ".join(current).strip()
                rows.append(
                    {
                        "source": name,
                        "chunk_id": chunk_id,
                        "chunk_text": chunk_text,
                    }
                )
                chunk_id += 1
                current = [s]
                current_len = len(s)
            else:
                current.append(s)
                current_len += len(s)

        if current:
            chunk_text = " ".join(current).strip()
            rows.append(
                {
                    "source": name,
                    "chunk_id": chunk_id,
                    "chunk_text": chunk_text,
                }
            )

    bq_client.query(f"TRUNCATE TABLE `{POLICY_CHUNKS_TABLE}`").result()

    if rows:
        errors = bq_client.insert_rows_json(POLICY_CHUNKS_TABLE, rows)
        if errors:
            raise RuntimeError(errors)


with DAG(
    dag_id="policy_process_dag",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
) as dag:

    chunk_task = PythonOperator(
        task_id="chunk_policies_to_bq",
        python_callable=chunk_policies_to_bq,
    )
