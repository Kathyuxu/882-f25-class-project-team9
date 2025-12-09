#!/bin/bash
set -e

PROJECT_ID="ba882-f25-class-project-team9"
SERVICE_NAME="basic-ml"
REGION="us-central1" 

gcloud config set project $PROJECT_ID

gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME .

gcloud run deploy $SERVICE_NAME \
  --image gcr.io/$PROJECT_ID/$SERVICE_NAME \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars GCP_PROJECT_ID=$PROJECT_ID
