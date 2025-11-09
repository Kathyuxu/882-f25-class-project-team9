import functions_framework
from google.cloud import bigquery
import pandas as pd
import json
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import numpy as np

project_id = 'ba882-f25-class-project-team9'

def get_bq_client():
    """Initialize BigQuery client"""
    return bigquery.Client(project=project_id)

@functions_framework.http
def task(request):
    """
    Cluster MBTA stations by last-mile connectivity
    
    Expected params:
    - run_id: training run identifier
    - dataset_id: dataset identifier
    - n_clusters: number of clusters (default: 3)
    """
    run_id = request.args.get("run_id")
    dataset_id = request.args.get("dataset_id")
    model_id = request.args.get("model_id", "model-transit-desert-clustering")
    n_clusters = int(request.args.get("n_clusters", 3))
    
    if not all([run_id, dataset_id]):
        return {"error": "Missing required parameters: run_id, dataset_id"}, 400
    
    try:
        bq = get_bq_client()
        
        # Load features from BigQuery
        query = f"""
        SELECT *
        FROM `{project_id}.bluebikes_analysis.mbta_clustering_features`
        """
        df = bq.query(query).to_dataframe()
        
        print(f"Loaded {len(df)} MBTA stations for clustering")
        
        # Feature columns for clustering
        feature_cols = [
            'distance_to_nearest_bluebikes_m',
            'bluebikes_within_500m',
            'bluebikes_nearby_morning',
            'avg_bikes_available_morning',
            'bluebikes_nearby_evening',
            'avg_bikes_available_evening',
            'bluebikes_trip_volume',
            'mbta_ridership',
            'is_isolated',
            'has_low_morning_availability'
        ]
        
        X = df[feature_cols].fillna(0)
        
        # Standardize features (CRITICAL for K-means!)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
       # Train K-means
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        df['cluster'] = kmeans.fit_predict(X_scaled)

        # ============ NEW: STATION-LEVEL LABELING ============
        # Label EACH STATION based on its OWN characteristics (not cluster averages)
        def label_station(row):
            distance = row['distance_to_nearest_bluebikes_m']
            morning_bikes = row['avg_bikes_available_morning']
            
            # Transit Desert: far away OR no bikes
            if distance > 800 or morning_bikes < 2:
                return "Transit Desert"
            
            # Well-Served: close AND plenty of bikes
            elif distance < 400 and morning_bikes > 5:
                return "Well-Served"
            
            # Moderate: everything else
            else:
                return "Moderate Coverage"

        df['cluster_label'] = df.apply(label_station, axis=1)

        # Calculate cluster statistics (for reporting)
        cluster_summary = {}
        for cluster_id in range(n_clusters):
            cluster_data = df[df['cluster'] == cluster_id]
            
            # Count stations by label within this cluster
            desert_count = len(cluster_data[cluster_data['cluster_label'] == 'Transit Desert'])
            served_count = len(cluster_data[cluster_data['cluster_label'] == 'Well-Served'])
            moderate_count = len(cluster_data[cluster_data['cluster_label'] == 'Moderate Coverage'])
            
            # Determine cluster's dominant label
            label_counts = {
                'Transit Desert': desert_count,
                'Well-Served': served_count,
                'Moderate Coverage': moderate_count
            }
            dominant_label = max(label_counts, key=label_counts.get)
            
            cluster_summary[str(cluster_id)] = {
                "label": dominant_label,
                "station_count": int(len(cluster_data)),
                "desert_count": desert_count,
                "served_count": served_count,
                "moderate_count": moderate_count,
                "avg_distance_to_bluebikes": float(cluster_data['distance_to_nearest_bluebikes_m'].mean()),
                "avg_morning_availability": float(cluster_data['avg_bikes_available_morning'].mean()),
                "avg_evening_availability": float(cluster_data['avg_bikes_available_evening'].mean()),
                "stations": cluster_data['station_name'].head(5).tolist()
            }
        
        # Label clusters based on characteristics
        cluster_labels = {}
        for cluster_id in range(n_clusters):
            cluster_data = df[df['cluster'] == cluster_id]
            avg_distance = cluster_data['distance_to_nearest_bluebikes_m'].mean()
            avg_morning_bikes = cluster_data['avg_bikes_available_morning'].mean()
            
            if avg_distance > 800 or avg_morning_bikes < 2:
                cluster_labels[cluster_id] = "Transit Desert"
            elif avg_morning_bikes > 5 and avg_distance < 400:
                cluster_labels[cluster_id] = "Well-Served"
            else:
                cluster_labels[cluster_id] = "Moderate Coverage"
        
        df['cluster_label'] = df['cluster'].map(cluster_labels)
        
        # Calculate cluster statistics
        cluster_summary = {}
        for cluster_id in range(n_clusters):
            cluster_data = df[df['cluster'] == cluster_id]
            cluster_summary[str(cluster_id)] = {
                "label": cluster_labels[cluster_id],
                "station_count": int(len(cluster_data)),
                "avg_distance_to_bluebikes": float(cluster_data['distance_to_nearest_bluebikes_m'].mean()),
                "avg_morning_availability": float(cluster_data['avg_bikes_available_morning'].mean()),
                "avg_evening_availability": float(cluster_data['avg_bikes_available_evening'].mean()),
                "stations": cluster_data['station_name'].head(5).tolist()
            }
        
        # Save results to BigQuery
        output_df = df[[
            'station_id', 'station_name', 'lat', 'lng', 
            'cluster', 'cluster_label',
            'distance_to_nearest_bluebikes_m',
            'avg_bikes_available_morning',
            'avg_bikes_available_evening'
        ]].copy()
        output_df['run_id'] = run_id
        
        table_id = f"{project_id}.bluebikes_analysis.mbta_station_clusters"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        )
        
        job = bq.load_table_from_dataframe(output_df, table_id, job_config=job_config)
        job.result()  # Wait for job to complete
        
        print(f"Saved {len(output_df)} clustered stations to {table_id}")
        
        return {
            "run_id": run_id,
            "algorithm": "kmeans",
            "n_clusters": n_clusters,
            "metrics": {
                "inertia": float(kmeans.inertia_),
                "silhouette_score": "not_calculated",
                "cluster_summary": cluster_summary,
                "total_stations": len(df)
            },
            "artifact": table_id
        }, 200
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}, 500