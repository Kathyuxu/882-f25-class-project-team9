"""
BA882 Utility Functions - BigQuery Version with Airflow Connection
"""

import os
from pathlib import Path
from google.cloud import bigquery
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


def get_bigquery_client():
    """
    Get a BigQuery client using Airflow connection
    """
    try:
        # Use Airflow connection for credentials
        hook = GoogleBaseHook(gcp_conn_id='clusteringML')
        credentials = hook.get_credentials()
        project_id = hook.project_id or 'ba882-f25-class-project-team9'
        
        return bigquery.Client(
            project=project_id,
            credentials=credentials
        )
    except Exception as e:
        print(f"Error getting BigQuery client: {e}")
        # Fallback to default credentials (for local testing)
        return bigquery.Client(project='ba882-f25-class-project-team9')


def read_sql(filepath: Path) -> str:
    """Read a SQL file from the given path"""
    if not filepath.exists():
        raise FileNotFoundError(f"SQL file not found: {filepath}")
    return filepath.read_text(encoding="utf-8")


def run_sql(sql: str):
    """Execute a SQL query and return results"""
    client = get_bigquery_client()
    query_job = client.query(sql)
    results = query_job.result()
    return [tuple(row.values()) for row in results]


def run_execute(sql: str):
    """Execute SQL statement(s) without returning results"""
    client = get_bigquery_client()
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    
    for statement in statements:
        print(f"Executing: {statement[:100]}...")
        query_job = client.query(statement)
        query_job.result()
        
    print(f"Successfully executed {len(statements)} statement(s)")