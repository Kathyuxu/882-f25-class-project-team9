gcloud config set project ba882-f25-class-project-team9

echo "======================================================"
echo "build (no cache)"
echo "======================================================"

docker build --no-cache -t gcr.io/ba882-f25-class-project-team9/streamlit-poc .

echo "======================================================"
echo "push"
echo "======================================================"

docker push gcr.io/ba882-f25-class-project-team9/streamlit-poc

echo "======================================================"
echo "deploy run"
echo "======================================================"

gcloud run deploy streamlit-poc \
    --image gcr.io/ba882-f25-class-project-team9/streamlit-poc \
    --platform managed \
    --region us-east1 \
    --allow-unauthenticated \
    --service-account composer-worker@ba882-f25-class-project-team9.iam.gserviceaccount.com \
    --memory 1Gi