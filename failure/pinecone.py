from datetime import datetime, timedelta
import os
import logging
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator

from google.cloud import storage
import vertexai
from vertexai.language_models import TextEmbeddingModel
from pinecone import Pinecone, ServerlessSpec

# ===================== CONFIG =====================

# GCP / Vertex
GCP_PROJECT_ID = "ba882-f25-class-project-team9"
GCP_LOCATION = "us-central1"

# GCS bucket & prefix（
BUCKET_NAME = "transit-policy-scraped-data"
PREFIX = "policies/"  

# Embedding Model
EMBED_MODEL = "text-embedding-004"
EMBED_DIM = 768

# Pinecone
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = "policy-embeddings-index"
PINECONE_CLOUD = "aws"
PINECONE_REGION = "us-east-1"

CHUNK_SIZE = 1000        
CHUNK_OVERLAP = 100      
EMBED_BATCH_SIZE = 8     

# ===================== HELPERS =====================

def init_vertex() -> None:
    vertexai.init(project=GCP_PROJECT_ID, location=GCP_LOCATION)


def list_gcs_files() -> List[str]:
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=PREFIX)
    files = [b.name for b in blobs if b.name.endswith(".txt")]
    logging.info(f"Found {len(files)} text files under gs://{BUCKET_NAME}/{PREFIX}")
    return files


def read_gcs_text(blob_name: str) -> str:
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    return blob.download_as_text()


def make_chunks(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP):
    chunks = []
    n = len(text)
    if n == 0:
        return chunks

    start = 0
    while start < n:
        end = min(start + size, n)
        chunk = text[start:end]
        chunks.append(chunk)
        start = end - overlap
        if start < 0:
            start = 0

    return chunks


def get_embeddings(texts):
    model = TextEmbeddingModel.from_pretrained(EMBED_MODEL)
    resp = model.get_embeddings(texts)
    return [r.values for r in resp]


def get_pinecone_index():
    if not PINECONE_API_KEY:
        raise ValueError("PINECONE_API_KEY is not set in environment variables")

    pc = Pinecone(api_key=PINECONE_API_KEY)
    existing = [i.name for i in pc.list_indexes()]

    if PINECONE_INDEX_NAME not in existing:
        logging.info(f"Creating Pinecone index: {PINECONE_INDEX_NAME}")
        pc.create_index(
            name=PINECONE_INDEX_NAME,
            dimension=EMBED_DIM,
            metric="cosine",
            spec=ServerlessSpec(
                cloud=PINECONE_CLOUD,
                region=PINECONE_REGION,
            ),
        )

    return pc.Index(PINECONE_INDEX_NAME)

# ===================== MAIN TASK =====================

def build_policy_embeddings(**context):

    logging.info("Initializing Vertex AI…")
    init_vertex()

    index = get_pinecone_index()

    files = list_gcs_files()
    total = 0

    for file_path in files:
        logging.info(f"Processing file: {file_path}")

        text = read_gcs_text(file_path)
        chunks = make_chunks(text)
        logging.info(f"{file_path} chunked into {len(chunks)} pieces")

        for start in range(0, len(chunks), EMBED_BATCH_SIZE):
            end = min(start + EMBED_BATCH_SIZE, len(chunks))
            batch_chunks = chunks[start:end]

            embeddings = get_embeddings(batch_chunks)

            vectors = []
            for i, (chunk, vec) in enumerate(zip(batch_chunks, embeddings)):
                global_chunk_id = start + i
                vector_id = f"{file_path}_chunk_{global_chunk_id}"

            
                vectors.append(
                    {
                        "id": vector_id,
                        "values": vec,
                        "metadata": {
                            "source": file_path,
                            "chunk_id": global_chunk_id,
                            "chunk_text": chunk[:1000],
                        },
                    }
                )
                total += 1

            logging.info(
                f"Upserting {len(vectors)} vectors for {file_path}, "
                f"chunks {start}–{end-1}"
            )
            index.upsert(vectors=vectors)

    logging.info(f"Finished. Total embeddings upserted: {total}")

# ===================== AIRFLOW DAG =====================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="policy_text_to_pinecone_embeddings",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule=None,   
    catchup=False,
    tags=["embedding", "pinecone", "policies"],
) as dag:

    build_embeddings_task = PythonOperator(
        task_id="build_policy_embeddings",
        python_callable=build_policy_embeddings,
    )

    build_embeddings_task


