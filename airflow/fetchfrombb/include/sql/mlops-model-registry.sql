-- Register a new model in the MLOps registry (BigQuery version)
INSERT INTO `ba882-f25-class-project-team9.bluebikes_analysis.mlops_model` (
    model_id,
    name,
    business_problem,
    ticket_number,
    owner,
    created_at
)
VALUES (
    '{{ model_id }}',
    '{{ name }}',
    '{{ business_problem }}',
    '{{ ticket_number }}',
    '{{ owner }}',
    CURRENT_TIMESTAMP()
);
