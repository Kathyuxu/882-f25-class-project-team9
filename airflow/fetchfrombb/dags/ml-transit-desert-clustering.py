from datetime import datetime
from airflow.decorators import dag, task
from pathlib import Path
import os
import json  # ← Make sure this is here!
import requests
from jinja2 import Template
from ba882 import utils

# Paths
BASE_DIR = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
SQL_DIR = BASE_DIR / "include" / "sql"


# Model metadata
model_vals = {
    "model_id": "model-transit-desert-clustering",
    "name": "MBTA Transit Desert Identification",
    "business_problem": "Identify MBTA stations with poor last-mile bike connectivity",
    "ticket_number": "TEAM9-ML-001",
    "owner": "Team 9"
}

def invoke_gcf(url: str, params: dict) -> dict:
    """Invoke Google Cloud Function"""
    response = requests.get(url, params=params, timeout=540)
    response.raise_for_status()
    return response.json()

@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlops", "clustering", "team9", "phase2"]
)
def transit_desert_clustering():
    
    @task
    def register_model():
        """Register the model in mlops_model table"""
        s = utils.read_sql(SQL_DIR / "mlops-model-registry.sql")
        template = Template(s)
        sql = template.render(**model_vals)
        print(sql)
        utils.run_execute(sql)
        return model_vals
    
    @task
    def create_clustering_features():
        """Create feature table in BigQuery"""
        print("Creating MBTA clustering features...")
        sql = utils.read_sql(SQL_DIR / "ai_datasets" / "mbta-clustering-features.sql")
        utils.run_execute(sql)
        
        # Get metadata
        result = utils.run_sql("""
            SELECT 
                COUNT(*) as row_count,
                COUNT(DISTINCT station_id) as unique_stations,
                14 as feature_count
            FROM `ba882-f25-class-project-team9.bluebikes_analysis.mbta_clustering_features`
        """)
        
        metadata = {
            "dataset_id": "ds-mbta-clustering-2025w08",
            "data_version": "2025_w08",
            "row_count": int(result[0][0]),
            "unique_stations": int(result[0][1]),
            "feature_count": int(result[0][2])
        }
        
        print(f"Dataset created: {metadata}")
        return metadata
    
    @task
    def register_dataset(dataset_metadata: dict, model_metadata: dict):
        """Register dataset in mlops_dataset table"""
        s = utils.read_sql(SQL_DIR / "mlops-dataset-registry.sql")
        template = Template(s)
        sql = template.render(
            model_id=model_metadata["model_id"],
            **dataset_metadata
        )
        print(sql)
        utils.run_execute(sql)
        return dataset_metadata
    
    @task
    def train_clustering_model(dataset_metadata: dict, model_metadata: dict):
        """Invoke Cloud Function to train clustering model"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_id = f"run_{dataset_metadata['data_version']}_kmeans_{timestamp}"
        
        url = "https://us-central1-ba882-f25-class-project-team9.cloudfunctions.net/ml-cluster-transit-deserts"
        params = {
            "run_id": run_id,
            "dataset_id": dataset_metadata['dataset_id'],
            "model_id": model_metadata['model_id'],
            "n_clusters": 3
        }
        
        print(f"Calling clustering function with run_id: {run_id}")
        result = invoke_gcf(url, params=params)
        
        return result
    
    @task
    def register_training_run(model_result: dict, dataset_metadata: dict, model_metadata: dict):
        """Register the clustering training run in mlops_training_run"""
        from google.cloud import bigquery
        
        params = {
            "algorithm": model_result["algorithm"],
            "n_clusters": model_result.get("n_clusters", 3),
            "model_type": "unsupervised_clustering"
        }
        
        metrics = {
            "inertia": model_result["metrics"]["inertia"],
            "total_stations": model_result["metrics"]["total_stations"],
            "cluster_summary": model_result["metrics"]["cluster_summary"]
        }
        
        # Convert to JSON strings (properly handled by BigQuery client)
        params_json = json.dumps(params)
        metrics_json = json.dumps(metrics)
        
        # Use parameterized query to avoid SQL injection and syntax issues
        sql = """
        INSERT INTO `ba882-f25-class-project-team9.bluebikes_analysis.mlops_training_run` (
            run_id, model_id, dataset_id, params, metrics, artifact, status, created_at
        )
        VALUES (
            @run_id,
            @model_id,
            @dataset_id,
            @params,
            @metrics,
            @artifact,
            'completed',
            CURRENT_TIMESTAMP()
        )
        """
        
        # Get BigQuery client
        from ba882.utils import get_bigquery_client
        client = get_bigquery_client()
        
        # Configure parameters
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", model_result['run_id']),
                bigquery.ScalarQueryParameter("model_id", "STRING", model_metadata['model_id']),
                bigquery.ScalarQueryParameter("dataset_id", "STRING", dataset_metadata['dataset_id']),
                bigquery.ScalarQueryParameter("params", "STRING", params_json),
                bigquery.ScalarQueryParameter("metrics", "STRING", metrics_json),
                bigquery.ScalarQueryParameter("artifact", "STRING", model_result['artifact']),
            ]
        )
        
        # Execute query
        query_job = client.query(sql, job_config=job_config)
        query_job.result()  # Wait for completion
        
        print(f"Registered training run: {model_result['run_id']}")
        return model_result
    
    # Define workflow
    model_task = register_model()
    features = create_clustering_features()
    register_ds = register_dataset(features, model_task)
    clustering = train_clustering_model(features, model_task)
    register_run = register_training_run(clustering, features, model_task)
    
    # Flow
    model_task >> features >> register_ds >> clustering >> register_run

transit_desert_clustering()