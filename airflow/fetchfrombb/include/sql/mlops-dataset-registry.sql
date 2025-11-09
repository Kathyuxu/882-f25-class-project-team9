-- Register a dataset version in the MLOps registry (BigQuery version)
INSERT INTO `ba882-f25-class-project-team9.bluebikes_analysis.mlops_dataset` (
    dataset_id,
    model_id,
    data_version,
    gcs_path,
    row_count,
    feature_count,
    created_at
)
VALUES (
    '{{ dataset_id }}',
    '{{ model_id }}',
    '{{ data_version }}',
    'ba882-f25-class-project-team9.bluebikes_analysis.mbta_clustering_features',
    {{ row_count }},
    {{ feature_count }},
    CURRENT_TIMESTAMP()
);