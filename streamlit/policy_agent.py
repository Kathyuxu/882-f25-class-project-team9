import numpy as np
from typing import List, Tuple

from google.cloud import bigquery

import vertexai
from vertexai.language_models import TextEmbeddingModel
from vertexai.generative_models import GenerativeModel, GenerationConfig


# ========= CONFIG =========
PROJECT_ID = "ba882-f25-class-project-team9"
LOCATION = "us-central1"

DATASET_LLM = "bluebikes_llm"
POLICY_CHUNKS_TABLE = f"{PROJECT_ID}.{DATASET_LLM}.policy_chunks"
POLICY_EMB_TABLE = f"{PROJECT_ID}.{DATASET_LLM}.policy_embeddings"

# Init Vertex AI
vertexai.init(project=PROJECT_ID, location=LOCATION)

# Models
tools = []
policy_model = GenerativeModel("gemini-2.5-flash", tools=tools)
emb_model = TextEmbeddingModel.from_pretrained("text-embedding-004")


# ========= LOAD EMBEDDINGS =========
def _load_policy_vectors() -> Tuple[np.ndarray, List[dict]]:
    """从 BigQuery 读所有 policy embedding + chunk_text"""
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT
            e.source,
            e.chunk_id,
            e.embedding,
            c.chunk_text
        FROM `{POLICY_EMB_TABLE}` AS e
        JOIN `{POLICY_CHUNKS_TABLE}` AS c
        USING (source, chunk_id)
        ORDER BY e.source, e.chunk_id
    """

    rows = list(client.query(query))
    if not rows:
        return np.zeros((0, 0)), []

    embeddings = []
    meta = []

    for r in rows:
        emb = np.array(r["embedding"], dtype="float32")
        embeddings.append(emb)

        meta.append({
            "source": r["source"],
            "chunk_id": r["chunk_id"],
            "chunk_text": r["chunk_text"],
        })

    embs = np.vstack(embeddings)
    norms = np.linalg.norm(embs, axis=1, keepdims=True) + 1e-9
    embs = embs / norms  # normalize

    return embs, meta


# Global in-memory cache
_POLICY_EMBS, _POLICY_META = None, None


def _ensure_loaded():
    global _POLICY_EMBS, _POLICY_META
    if _POLICY_EMBS is None or _POLICY_META is None:
        _POLICY_EMBS, _POLICY_META = _load_policy_vectors()


# ========= EMBED QUERY =========
def _embed_query(text: str) -> np.ndarray:
    emb = emb_model.get_embeddings([text])[0].values
    v = np.array(emb, dtype="float32")
    v = v / (np.linalg.norm(v) + 1e-9)
    return v


# ========= RAG RETRIEVAL =========
def _retrieve_top_k(question: str, k: int = 5) -> List[dict]:
    _ensure_loaded()
    if _POLICY_EMBS.shape[0] == 0:
        return []

    q_vec = _embed_query(question)
    scores = _POLICY_EMBS @ q_vec  # cosine similarity
    topk_idx = np.argsort(-scores)[:k]

    results = []
    for idx in topk_idx:
        item = _POLICY_META[idx].copy()
        item["score"] = float(scores[idx])
        results.append(item)

    return results


# ========= MAIN AGENT CALL =========
def ask_policy_agent(question: str) -> str:
    """Main entry: RAG + Gemini Answer."""
    contexts = _retrieve_top_k(question, k=5)

    if not contexts:
        context_text = "No policy context found in database."
    else:
        context_text = "\n\n---\n\n".join(
            f"[source={c['source']}, chunk_id={c['chunk_id']}, score={c['score']:.3f}]\n{c['chunk_text']}"
            for c in contexts
        )

    system_instruction = (
        "You are a Bluebikes & MBTA policy assistant. "
        "Answer ONLY using the provided policy excerpts. "
        "If the answer is not clearly supported, say you are not sure and "
        "recommend checking the official Bluebikes or MBTA website."
    )

    user_content = (
        f"User question:\n{question}\n\n"
        f"Relevant policy excerpts:\n{context_text}"
    )

    response = policy_model.generate_content(
        contents=[system_instruction, user_content],
        generation_config=GenerationConfig(
            temperature=0.1,
            max_output_tokens=512,
        ),
    )

    try:
        return response.text.strip()
    except:
        return "".join(p.text for p in response.candidates[0].content.parts).strip()
