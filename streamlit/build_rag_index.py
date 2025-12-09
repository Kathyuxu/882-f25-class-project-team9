# build_rag_index.py

from google.cloud import bigquery
from google import genai
import pandas as pd
import math

PROJECT_ID = "ba882-f25-class-project-team9"
SOURCE_TABLE = f"{PROJECT_ID}.mbta_data.stations_dedup"
TARGET_TABLE = f"{PROJECT_ID}.mbta_rag.docs"

MAX_BATCH = 250   # <-- Vertex AI limit


def fetch_source_rows(bqclient):
    query = f"""
    SELECT
      station_id,
      ANY_VALUE(station_name) AS station_name,
      ANY_VALUE(lat) AS lat,
      ANY_VALUE(lng) AS lng
    FROM `{SOURCE_TABLE}`
    WHERE station_id IS NOT NULL
    GROUP BY station_id
    """
    return bqclient.query(query).to_dataframe()


def build_text_rows(df):
    records = []
    for _, row in df.iterrows():
        text = (
            f"MBTA station {row['station_name']} (ID {row['station_id']}) "
            f"is located at latitude {row['lat']} and longitude {row['lng']}."
        )
        records.append({
            "id": f"station_{row['station_id']}",
            "text": text,
            "source_type": "station",
            "stop_id": row["station_id"],
            "route_id": None,
        })
    return pd.DataFrame(records)


# ----------------------------------------------------
# FIXED BATCH EMBEDDING
# ----------------------------------------------------
def embed_texts(genai_client, texts):
    all_embeddings = []

    total = len(texts)
    num_batches = math.ceil(total / MAX_BATCH)

    print(f"Embedding {total} texts in {num_batches} batches…")

    for i in range(num_batches):
        batch = texts[i * MAX_BATCH : (i + 1) * MAX_BATCH]
        print(f"  → Batch {i+1}/{num_batches} (size={len(batch)})")

        resp = genai_client.models.embed_content(
            model="text-embedding-005",
            contents=batch       # <-- list[str]
        )
        batch_embs = [emb.values for emb in resp.embeddings]
        all_embeddings.extend(batch_embs)

    return all_embeddings


def main():
    print("Fetching BigQuery rows…")
    bqclient = bigquery.Client(project=PROJECT_ID)

    print("Initializing Vertex AI client…")
    genai_client = genai.Client(
        vertexai=True,
        project=PROJECT_ID,
        location="us-central1"
    )

    src_df = fetch_source_rows(bqclient)
    df = build_text_rows(src_df)

    print(f"Total rows to embed: {len(df)}")

    df["embedding"] = embed_texts(genai_client, df["text"].tolist())

    print("Uploading to BigQuery…")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bqclient.load_table_from_dataframe(df, TARGET_TABLE, job_config=job_config)
    job.result()

    print(f"✔ Loaded {len(df)} rows into {TARGET_TABLE}")


if __name__ == "__main__":
    main()
