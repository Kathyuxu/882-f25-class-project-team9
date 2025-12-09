import os
import json
import requests
import pandas as pd
import streamlit as st
import pydeck as pdk
import altair as alt
from google.cloud import bigquery
from assistant import ask_agent
from policy_agent import ask_policy_agent
from google import genai
from multi_agent import ask_unified_agent

st.set_page_config(page_title="MBTA × Bluebikes — Last-Mile", layout="wide")

# ----------------------- CONFIG -----------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ba882-f25-class-project-team9")
T_MBTA_STATIONS = f"{PROJECT_ID}.mbta_data.stations_dedup"
T_BB_INFO      = f"{PROJECT_ID}.bluebikes_analysis.station_info"
T_BB_STATUS    = f"{PROJECT_ID}.bluebikes_analysis.station_status"
T_BB_HIST      = f"{PROJECT_ID}.bluebikes_historical_us.JantoSep_historical"
T_PRED_PICKUPS = f"{PROJECT_ID}.bluebikes_analysis.golden_station_hourly_pickups"
T_PRED_DROPS   = f"{PROJECT_ID}.bluebikes_analysis.golden_station_hourly_dropoffs"
T_CLUSTERS     = f"{PROJECT_ID}.bluebikes_analysis.mbta_station_clusters"

client = bigquery.Client(project=PROJECT_ID)

# ---- Gemini & RAG / MBTA API for AI Transit Assistant ----
genai_client = genai.Client(
    vertexai=True,
    project=PROJECT_ID,
    location="us-east5"
)
MBTA_API_KEY = os.getenv("MBTA_API_KEY")

MBTA_STOP_ADDRESSES = {
    "Park Street": "Park Street, Boston, MA",
    "Park Street station": "Park Street, Boston, MA",
    "Sutherland Road": "Sutherland Road, Brighton, MA",
    "Sutherland Road station": "Sutherland Road, Brighton, MA",
    "Chiswick Road": "Chiswick Road, Brighton, MA",
    "Chestnut Hill Avenue": "Chestnut Hill Avenue, Brighton, MA",
}

# ----------------------- CACHED QUERY -----------------------
@st.cache_data(ttl=600)
def run_query(sql: str, _params=None, cache_key: str = "") -> pd.DataFrame:
    _ = cache_key
    job_config = bigquery.QueryJobConfig(query_parameters=_params or [])
    return client.query(sql, job_config=job_config).to_dataframe()

# ----------------------- SIDEBAR -----------------------
st.sidebar.header("Filters")
miles = st.sidebar.slider("Radius (miles)", 0.25, 2.0, 1.0, 0.25)
meters = miles * 1609.34
use_avail = st.sidebar.checkbox(
    "Require historical availability (≥30% active last 14 days)", value=False
)

st.sidebar.markdown("---")
page = st.sidebar.radio(
    "Pages",
    [
        "Review_MBTA",
        "Review_BB",
        "Overview",
        "Summary",
        "Recommendations",
        "Occupancy Prediction",
        "Bluebikes Clustering",
        "Bluebikes Demand Prediction",
        "Availability",
        "AI Transit Assistant",
        "Bluebikes & Policy Assistant", 
    ]
)

# ----------------------- CORE COVERAGE SQL -----------------------
SQL_CORE = f"""
WITH mbta AS (
  SELECT
    station_id,
    station_name,
    CAST(lat AS FLOAT64) AS lat,
    CAST(lng AS FLOAT64) AS lng,
    ST_GEOGPOINT(CAST(lng AS FLOAT64), CAST(lat AS FLOAT64)) AS geog
  FROM `{T_MBTA_STATIONS}`
),
bb_latest AS (
  SELECT
    station_id AS bb_station_id,
    name       AS bb_name,
    CAST(lat AS FLOAT64) AS lat,
    CAST(lon AS FLOAT64) AS lon,
    ST_GEOGPOINT(CAST(lon AS FLOAT64), CAST(lat AS FLOAT64)) AS geog
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY snapshot_ts DESC) rn
    FROM `{T_BB_INFO}`
  )
  WHERE rn = 1
),
pairs AS (
  SELECT
    m.station_id, m.station_name,
    m.lat AS mbta_lat, m.lng AS mbta_lng,
    b.bb_station_id,
    CASE WHEN b.bb_station_id IS NOT NULL THEN ST_DISTANCE(m.geog, b.geog) END AS distance_m
  FROM mbta m
  LEFT JOIN bb_latest b
    ON ST_DWITHIN(m.geog, b.geog, @meters)
)
SELECT
  station_id,
  ANY_VALUE(station_name) AS station_name,
  ANY_VALUE(mbta_lat)     AS mbta_lat,
  ANY_VALUE(mbta_lng)     AS mbta_lng,
  COUNT(DISTINCT bb_station_id) AS nearby_bluebikes_count,
  MIN(distance_m)              AS nearest_distance_m,
  (COUNT(DISTINCT bb_station_id) >= 1) AS meets_last_mile
FROM pairs
GROUP BY station_id
ORDER BY meets_last_mile DESC, nearest_distance_m;
"""

SQL_WITH_AVAIL = f"""
WITH window AS (
  SELECT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY) AS start_ts
),
bb_latest AS (
  SELECT
    station_id AS bb_station_id,
    name       AS bb_name,
    CAST(lat AS FLOAT64) AS lat,
    CAST(lon AS FLOAT64) AS lon,
    ST_GEOGPOINT(CAST(lon AS FLOAT64), CAST(lat AS FLOAT64)) AS geog
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY snapshot_ts DESC) rn
    FROM `{T_BB_INFO}`
  ) WHERE rn = 1
),
bb_avail AS (
  SELECT
    station_id AS bb_station_id,
    AVG( IFNULL(num_bikes_available,0) > 0 ) AS active_ratio
  FROM `{T_BB_STATUS}`, window
  WHERE snapshot_ts >= window.start_ts
  GROUP BY bb_station_id
),
mbta AS (
  SELECT
    station_id,
    station_name,
    CAST(lat AS FLOAT64) AS lat,
    CAST(lng AS FLOAT64) AS lng,
    ST_GEOGPOINT(CAST(lng AS FLOAT64), CAST(lat AS FLOAT64)) AS geog
  FROM `{T_MBTA_STATIONS}`
),
pairs AS (
  SELECT
    m.station_id, m.station_name,
    m.lat AS mbta_lat, m.lng AS mbta_lng,
    b.bb_station_id,
    CASE WHEN b.bb_station_id IS NOT NULL THEN ST_DISTANCE(m.geog, b.geog) END AS distance_m,
    a.active_ratio
  FROM mbta m
  LEFT JOIN bb_latest b
    ON ST_DWITHIN(m.geog, b.geog, @meters)
  LEFT JOIN bb_avail a
    ON a.bb_station_id = b.bb_station_id
)
SELECT
  station_id,
  ANY_VALUE(station_name) AS station_name,
  ANY_VALUE(mbta_lat)     AS mbta_lat,
  ANY_VALUE(mbta_lng)     AS mbta_lng,
  COUNTIF(IFNULL(active_ratio,0) >= 0.3) AS nearby_active_bluebikes,
  MIN(CASE WHEN IFNULL(active_ratio,0) >= 0.3 THEN distance_m END) AS nearest_distance_m,
  (COUNTIF(IFNULL(active_ratio,0) >= 0.3) >= 1) AS meets_last_mile
FROM pairs
GROUP BY station_id
ORDER BY meets_last_mile DESC, nearest_distance_m;
"""

# ----------------------- AVAILABILITY PAGE SQL -----------------------
SQL_NEAREST_AVAIL = f"""
WITH m AS (
  SELECT
    station_id,
    station_name,
    CAST(lat AS FLOAT64) AS lat,
    CAST(lng AS FLOAT64) AS lng,
    ST_GEOGPOINT(CAST(lng AS FLOAT64), CAST(lat AS FLOAT64)) AS geog
  FROM `{T_MBTA_STATIONS}`
  WHERE station_id = @mbta_station_id
),
bb_latest AS (
  SELECT
    station_id AS bb_station_id,
    name AS bb_name,
    CAST(lat AS FLOAT64) AS lat,
    CAST(lon AS FLOAT64) AS lon,
    ST_GEOGPOINT(CAST(lon AS FLOAT64), CAST(lat AS FLOAT64)) AS geog
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY snapshot_ts DESC) rn
    FROM `{T_BB_INFO}`
  ) WHERE rn = 1
),
bb_status_latest AS (
  SELECT
    station_id AS bb_station_id,
    num_bikes_available,
    num_docks_available,
    is_renting,
    is_returning,
    snapshot_ts AS status_snapshot_ts
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY snapshot_ts DESC) rn
    FROM `{T_BB_STATUS}`
  ) WHERE rn = 1
),
nearest AS (
  SELECT
    m.station_id AS mbta_station_id,
    m.station_name AS mbta_station_name,
    m.lat AS mbta_lat, m.lng AS mbta_lng,
    b.bb_station_id, b.bb_name,
    b.lat AS bb_lat, b.lon AS bb_lng,
    ST_DISTANCE(m.geog, b.geog) AS distance_m
  FROM m
  CROSS JOIN bb_latest b
  ORDER BY distance_m
  LIMIT 1
)
SELECT
  n.*,
  s.num_bikes_available,
  s.num_docks_available,
  s.is_renting,
  s.is_returning,
  s.status_snapshot_ts
FROM nearest n
LEFT JOIN bb_status_latest s
  ON s.bb_station_id = n.bb_station_id
"""

# ----------------------- RUN CORE COVERAGE QUERY -----------------------
sql = SQL_WITH_AVAIL if use_avail else SQL_CORE
params = [bigquery.ScalarQueryParameter("meters", "FLOAT64", meters)]

with st.spinner("Querying BigQuery…"):
    df = run_query(sql, params, cache_key=f"core::{use_avail}::{meters:.2f}")

if df.empty:
    st.warning("No results found. Try increasing the radius.")
    st.stop()

if "nearest_distance_m" in df.columns:
    df["nearest_distance_m"] = df["nearest_distance_m"].astype(float).round(1)

meets = df[df["meets_last_mile"]].sort_values("nearest_distance_m", na_position="last")
fails = df[~df["meets_last_mile"]].sort_values("nearest_distance_m", na_position="last")

# ----------------------- MAP HELPER -----------------------
def render_map(df_sub, rgba, title):
    st.subheader(title)
    if df_sub.empty:
        st.info("No stations to display.")
        return
    points = pd.DataFrame({
        "lat": df_sub["mbta_lat"],
        "lon": df_sub["mbta_lng"],
        "name": df_sub["station_name"],
        "color": [rgba] * len(df_sub)
    })
    view = pdk.ViewState(
        latitude=points["lat"].mean() if points["lat"].notna().any() else 42.3601,
        longitude=points["lon"].mean() if points["lon"].notna().any() else -71.0589,
        zoom=10
    )
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=points,
        get_position='[lon, lat]',
        get_fill_color='color',
        get_radius=60,
        pickable=True
    )
    st.pydeck_chart(pdk.Deck(initial_view_state=view, layers=[layer], tooltip={"text": "{name}"}))

# ----------------------- AI HELPER FUNCTIONS -----------------------
def geocode_place(place_text: str):
    place_text = place_text.strip().lower()

    sql = f"""
        SELECT
            station_name,
            CAST(lat AS FLOAT64) AS lat,
            CAST(lng AS FLOAT64) AS lng
        FROM `{T_MBTA_STATIONS}`
        WHERE REGEXP_CONTAINS(LOWER(station_name), @name)
        LIMIT 1
    """

    df_geo = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("name", "STRING", place_text)
            ]
        )
    ).to_dataframe()

    if df_geo.empty:
        print("❌ NO MATCH FOR:", place_text)
        return None, None

    print("✅ MATCH FOUND:", df_geo.iloc[0]["station_name"])
    return df_geo.iloc[0]["lat"], df_geo.iloc[0]["lng"]


def find_nearest_station(lat: float, lon: float):
    q = f"""
        SELECT
            station_id,
            ANY_VALUE(station_name) AS station_name,
            ANY_VALUE(lat) AS station_lat,
            ANY_VALUE(lng) AS station_lng,
            SQRT(POW(ANY_VALUE(lat) - {lat}, 2) + POW(ANY_VALUE(lng) - {lon}, 2)) AS dist
        FROM `{T_MBTA_STATIONS}`
        WHERE station_id IS NOT NULL
        GROUP BY station_id
        ORDER BY dist
        LIMIT 1
    """
    df_near = client.query(q).to_dataframe()
    if df_near.empty:
        return None, None
    row = df_near.iloc[0]
    return row["station_id"], row["station_name"]


def extract_origin_destination(user_q: str):
    prompt = f"""
    You will extract ORIGIN and DESTINATION from the user's text.

    Return ONLY valid JSON.
    No extra text. No comments. No backticks.

    Format:
    {{
      "origin": "<short place name>",
      "destination": "<short place name>"
    }}

    User text: "{user_q}"
    """

    out = genai_client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )

    raw = out.text.strip()
    raw = raw.replace("```json", "").replace("```", "").strip()

    import re

    try:
        data = json.loads(raw)
        return data.get("origin"), data.get("destination")
    except Exception:
        pass

    try:
        match = re.search(r"{.*}", raw, re.DOTALL)
        if match:
            data = json.loads(match.group(0))
            return data.get("origin"), data.get("destination")
    except Exception:
        pass

    return None, None


def rag_retrieve_context(q_text: str, top_k=5):
    emb = genai_client.models.embed_content(
        model="text-embedding-005",
        contents=q_text
    ).embeddings[0].values

    query = """
    SELECT text,
           ML.DISTANCE(embedding, @emb, 'COSINE') AS score
    FROM `ba882-f25-class-project-team9.mbta_rag.docs`
    ORDER BY score ASC
    LIMIT @top_k
    """

    df_rag = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("emb", "FLOAT64", emb),
                bigquery.ScalarQueryParameter("top_k", "INT64", top_k)
            ]
        )
    ).to_dataframe()

    return "\n".join(df_rag["text"]) if not df_rag.empty else ""


def get_live_departures(stop_id: str, max_results=3):
    if not MBTA_API_KEY:
        return []

    url = "https://api-v3.mbta.com/predictions"
    params = {
        "api_key": MBTA_API_KEY,
        "filter[stop]": stop_id,
        "sort": "departure_time",
        "page[limit]": max_results,
    }
    try:
        r = requests.get(url, params=params, timeout=5).json()
        out = []
        for d in r.get("data", []):
            attrs = d["attributes"]
            out.append({
                "departure_time": attrs.get("departure_time"),
                "status": attrs.get("status")
            })
        return out
    except Exception:
        return []


def generate_final_answer(user_query, origin, dest,
                          o_station, d_station,
                          occupancy, departures, rag_text):

    dep_txt = "\n".join(
        f"- {d['departure_time']} ({d['status']})"
        for d in departures
    ) or "No live data."

    prompt = f"""
    You are an MBTA travel assistant.

    User asked: {user_query}
    Parsed origin: {origin}
    Parsed destination: {dest}

    Nearest origin station: {o_station}
    Nearest destination station: {d_station}

    Predicted occupancy: {occupancy}

    Live departures:
    {dep_txt}

    Helpful context:
    {rag_text}

    Give a clear and short route suggestion.
    """

    out = genai_client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )

    return out.text

# ----------------------- CLASSIFY FAILING STATIONS -----------------------
KEYWORDS_OUTER_CENTER = ("Center", "City Hall", "Downtown", "Square", "Sq")
KEYWORDS_SEASONAL = ("Beach", "Harbor", "Greenbush", "Newburyport", "Gloucester")
KEYWORDS_FAR_END = ("Route", "Parking Lot", "495", "Park", "Terminal", "End")

def classify_station(name: str) -> str:
    n = (name or "").lower()
    if any(k.lower() in n for k in KEYWORDS_OUTER_CENTER):
        return "outer_city_center"
    if any(k.lower() in n for k in KEYWORDS_SEASONAL):
        return "seasonal_or_tourism"
    if any(k.lower() in n for k in KEYWORDS_FAR_END):
        return "far_end_or_park_and_ride"
    return "other"

fails["category"] = fails["station_name"].apply(classify_station)
cat_counts = fails["category"].value_counts().to_dict()

# ===================== PAGES =====================
st.title("🚲 MBTA × Bluebikes — Last-Mile Coverage")
st.caption(
    f"Rule: ≥1 Bluebikes station within {miles:.2f} mile(s) (~{meters:.0f} m). "
    + ("(Availability rule applied)" if use_avail else "")
)

# ----------------------- PAGE: Summary -----------------------
if page == "Summary":
    st.header("📊 Descriptive Summary")

    total_stations = len(df)
    num_meets = len(meets)
    num_fails = len(fails)
    percent_meets = (num_meets / total_stations * 100) if total_stations > 0 else 0.0

    median_distance = (
        df["nearest_distance_m"].median()
        if "nearest_distance_m" in df.columns and df["nearest_distance_m"].notna().any()
        else None
    )
    mean_distance = (
        df["nearest_distance_m"].mean()
        if "nearest_distance_m" in df.columns and df["nearest_distance_m"].notna().any()
        else None
    )
    p90_distance = (
        df["nearest_distance_m"].quantile(0.90)
        if "nearest_distance_m" in df.columns and df["nearest_distance_m"].notna().any()
        else None
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total MBTA Stations (filtered)", total_stations)
    c2.metric("Stations Meeting Rule", num_meets)
    c3.metric("Stations NOT Meeting Rule", num_fails)
    c4.metric("Compliance Rate", f"{percent_meets:.1f}%")

    bullets = []
    if median_distance is not None:
        bullets.append(f"- Median nearest Bluebikes distance: **{median_distance:.1f} m**")
    if mean_distance is not None:
        bullets.append(f"- Mean nearest Bluebikes distance: **{mean_distance:.1f} m**")
    if p90_distance is not None:
        bullets.append(f"- 90th percentile of distance: **{p90_distance:.1f} m**")
    bullets.append(f"- Availability rule: **{'ON (≥30% active last 14 days)' if use_avail else 'OFF'}**")
    bullets.append(f"- Radius rule: **≥1 station within {miles:.2f} mi (~{meters:.0f} m)**")
    st.markdown("\n".join(bullets))

    st.markdown("---")

    if "nearest_distance_m" in df.columns and df["nearest_distance_m"].notna().any():
        c5, c6 = st.columns(2)
        with c5:
            st.subheader("Nearest 10 (m)")
            st.dataframe(
                df.sort_values("nearest_distance_m", na_position="last")
                  .loc[:, ["station_id", "station_name", "nearest_distance_m"]]
                  .head(10)
                  .rename(columns={"nearest_distance_m": "nearest_m"}),
                use_container_width=True
            )
        with c6:
            st.subheader("Farthest 10 (m)")
            st.dataframe(
                df.sort_values("nearest_distance_m", ascending=False, na_position="last")
                  .loc[:, ["station_id", "station_name", "nearest_distance_m"]]
                  .head(10)
                  .rename(columns={"nearest_distance_m": "nearest_m"}),
                use_container_width=True
            )

    st.stop()

# ----------------------- PAGE: Overview -----------------------
elif page == "Overview":
    c1, c2, c3 = st.columns(3)
    c1.metric("Stations meeting rule", len(meets))
    c2.metric("Stations NOT meeting rule", len(fails))
    c3.metric("Median nearest distance (m)",
              f"{df['nearest_distance_m'].median():.0f}" if df['nearest_distance_m'].notna().any() else "—")

    st.subheader("✅ Stations meeting the last-mile rule")
    st.dataframe(
        meets[["station_id", "station_name", "nearby_bluebikes_count", "nearest_distance_m"]]
          .rename(columns={"nearest_distance_m": "nearest_m"}),
        use_container_width=True
    )

    st.subheader("❌ Stations NOT meeting the last-mile rule")
    st.dataframe(
        fails[["station_id", "station_name"]],
        use_container_width=True
    )

    render_map(meets, [0, 180, 0, 200], "🗺️ Map — Stations meeting the rule")
    render_map(fails, [200, 0, 0, 220], "🗺️ Map — Stations NOT meeting the rule")

# ----------------------- PAGE: Recommendations -----------------------
elif page == "Recommendations":
    st.header("💡 Recommendations")

    st.markdown("#### Summary Snapshot")
    st.markdown(f"""
- **Failing MBTA stations:** {len(fails)}
- **Passing MBTA stations:** {len(meets)}
- **Categories (heuristic classification):**
  - Outer city centers: **{cat_counts.get('outer_city_center', 0)}**
  - Seasonal / tourism: **{cat_counts.get('seasonal_or_tourism', 0)}**
  - Far-end / Park & Ride: **{cat_counts.get('far_end_or_park_and_ride', 0)}**
  - Other: **{cat_counts.get('other', 0)}**
""")

    st.markdown("#### Recommended Actions")
    st.markdown("""
- **Short-term (0–6 months)**
  - Pilot **outer city centers** (e.g., Quincy Center, Woburn City Hall) with 1–2 docks near MBTA stations.
  - Add a **“Commuter Rail + Bluebikes” pass/discount** in MBTA apps.
- **Medium-term (6–24 months)**
  - For **Far-end / Park & Ride** stations, roll out **Park-and-Bike** and evaluate ridership before adding docks.
  - Expand to regional hubs when infrastructure is ready.
- **Long-term (24+ months)**
  - Deploy **seasonal docks** in tourism corridors for Apr–Oct.
  - Build a **multi-modal** network with micromobility (e-scooters, carshare) for suburban coverage.
""")

    st.markdown("#### Map of Non-Compliant MBTA Stations")
    render_map(fails, [200, 0, 0, 220], "🗺️ MBTA Stations Not Meeting Last-Mile Rule")

    with st.expander("See list of failing stations"):
        st.dataframe(fails[["station_id", "station_name"]], use_container_width=True)

# ----------------------- PAGE: Occupancy Prediction -----------------------
elif page == "Occupancy Prediction":
    st.header(":bus: MBTA Vehicle Occupancy Prediction (via BigQuery ML)")
    st.caption("Predict real-time train/bus occupancy levels using the deployed Logistic Regression model.")

    route_id = st.text_input("Enter Route ID (e.g., Red, 1, 47)", "Red")
    direction_id = st.selectbox("Direction", [0, 1], index=0)
    hour_of_day = st.slider("Hour of Day", 0, 23, 8)

    day_map = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6
    }
    day_of_week_label = st.selectbox("Day of Week", list(day_map.keys()))
    day_of_week = day_map[day_of_week_label]

    current_status = st.selectbox("Current Status", ["IN_TRANSIT", "STOPPED_AT", "INCOMING_AT"])

    SQL_FETCH_COORDS = f"""
    SELECT
      AVG((start_lat + end_lat) / 2.0) AS lat,
      AVG((start_lng + end_lng) / 2.0) AS lon
    FROM `{PROJECT_ID}.mbta_data.mbta_historical`
    WHERE LOWER(route_id) = LOWER(@rid)
    """
    coords = run_query(
        SQL_FETCH_COORDS,
        [bigquery.ScalarQueryParameter("rid", "STRING", route_id)],
        cache_key=f"coords::{route_id}"
    )

    if not coords.empty and not coords.isnull().values.any():
        lat = float(coords.iloc[0]["lat"])
        lon = float(coords.iloc[0]["lon"])
        lat_bin = float(round(lat, 1))
        lon_bin = float(round(lon, 1))
        st.info(f"Fetched coordinates: lat={lat:.4f}, lon={lon:.4f} → bins ({lat_bin}, {lon_bin})")
    else:
        st.warning(f"No coordinates found for route '{route_id}'. Using default bins.")
        lat_bin, lon_bin = 10.0, 20.0

    SQL_OCC_PRED = f"""
    SELECT
      predicted_label,
      predicted_label_probs
    FROM ML.PREDICT(MODEL `{PROJECT_ID}.mbta_ml.occ_lr_min`,
      (SELECT
          @rid AS route_id,
          @status AS current_status,
          @hour AS hour_of_day,
          @dow AS day_of_week,
          @is_wknd AS is_weekend,
          @lat_bin AS lat_bin,
          @lon_bin AS lon_bin
      ))
    """

    if st.button(":crystal_ball: Run Prediction"):
        with st.spinner("Querying BigQuery ML model..."):
            try:
                df_pred = run_query(
                    SQL_OCC_PRED,
                    [
                        bigquery.ScalarQueryParameter("rid", "STRING", route_id),
                        bigquery.ScalarQueryParameter("status", "STRING", current_status),
                        bigquery.ScalarQueryParameter("hour", "INT64", int(hour_of_day)),
                        bigquery.ScalarQueryParameter("dow", "INT64", int(day_of_week)),
                        bigquery.ScalarQueryParameter("is_wknd", "INT64", 1 if day_of_week in [5, 6] else 0),
                        bigquery.ScalarQueryParameter("lat_bin", "FLOAT64", float(lat_bin)),
                        bigquery.ScalarQueryParameter("lon_bin", "FLOAT64", float(lon_bin)),
                    ],
                    cache_key=f"occ_pred::{route_id}::{direction_id}::{day_of_week}::{hour_of_day}::{current_status}"
                )

                if not df_pred.empty:
                    pred_label = df_pred.iloc[0]["predicted_label"]
                    st.success(f"### :train2: Predicted Occupancy: **{pred_label}**")

                    probs = df_pred.iloc[0].get("predicted_label_probs", None)
                    if probs is not None:
                        try:
                            prob_df = pd.DataFrame(probs)
                            if {"label", "prob"}.issubset(set(prob_df.columns)):
                                prob_df = prob_df.sort_values("prob", ascending=False)
                            st.subheader("Class Probabilities")
                            st.dataframe(prob_df, use_container_width=True)
                        except Exception:
                            st.write(probs)
                else:
                    st.warning("No prediction result returned from BigQuery ML model.")
            except Exception as e:
                st.error(f"Error running ML prediction: {e}")

# ----------------------- PAGE: Review_MBTA -----------------------
elif page == "Review_MBTA":
    st.header("🚆 MBTA — System Review (Historical)")
    st.caption("Source: mbta_data.mbta_historical")

    YEAR = st.selectbox("Select Year", [2023, 2024, 2025], index=2)
    params_year = [bigquery.ScalarQueryParameter("yy", "INT64", YEAR)]

    sql_total = f"""
        SELECT COUNT(1) AS total_trips
        FROM `{PROJECT_ID}.mbta_data.mbta_historical`
        WHERE EXTRACT(YEAR FROM scheduled_start_time) = @yy
    """
    total_trips = int(run_query(sql_total, params_year, cache_key=f"mbta_total::{YEAR}").iloc[0]["total_trips"])

    sql_top_routes = f"""
        SELECT route_id, COUNT(1) AS trips
        FROM `{PROJECT_ID}.mbta_data.mbta_historical`
        WHERE EXTRACT(YEAR FROM scheduled_start_time) = @yy
        GROUP BY route_id ORDER BY trips DESC LIMIT 10
    """
    df_top_routes = run_query(sql_top_routes, params_year, cache_key=f"mbta_routes::{YEAR}")

    sql_top_starts = f"""
        SELECT start_station_name AS station, COUNT(1) AS trips
        FROM `{PROJECT_ID}.mbta_data.mbta_historical`
        WHERE EXTRACT(YEAR FROM scheduled_start_time) = @yy
        GROUP BY station ORDER BY trips DESC LIMIT 10
    """
    df_top_starts = run_query(sql_top_starts, params_year, cache_key=f"mbta_starts::{YEAR}")

    sql_monthly = f"""
        SELECT
          FORMAT_TIMESTAMP('%B', scheduled_start_time, 'America/New_York') AS month,
          COUNT(1) AS trips
        FROM `{PROJECT_ID}.mbta_data.mbta_historical`
        WHERE EXTRACT(YEAR FROM scheduled_start_time) = @yy
        GROUP BY month
    """
    df_monthly = run_query(sql_monthly, params_year, cache_key=f"mbta_month::{YEAR}")
    month_order = [
        "January","February","March","April","May","June",
        "July","August","September","October","November","December"
    ]
    if not df_monthly.empty:
        df_monthly["month"] = pd.Categorical(df_monthly["month"], categories=month_order, ordered=True)

    sql_season = f"""
        SELECT
          CASE
            WHEN EXTRACT(MONTH FROM scheduled_start_time) IN (12,1,2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM scheduled_start_time) IN (3,4,5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM scheduled_start_time) IN (6,7,8) THEN 'Summer'
            ELSE 'Fall' END AS season,
          route_type,
          COUNT(1) AS trips
        FROM `{PROJECT_ID}.mbta_data.mbta_historical`
        WHERE EXTRACT(YEAR FROM scheduled_start_time) = @yy
        GROUP BY season, route_type
    """
    df_season = run_query(sql_season, params_year, cache_key=f"mbta_season::{YEAR}")
    if not df_season.empty:
        df_season["season"] = pd.Categorical(df_season["season"], categories=["Fall","Winter","Spring","Summer"], ordered=True)

    st.subheader("🛣️ Top Routes (by Trip Volume)")
    if df_top_routes.empty:
        st.info("No data.")
    else:
        st.altair_chart(
            alt.Chart(df_top_routes).mark_bar().encode(
                x=alt.X("trips:Q", title="Trips"),
                y=alt.Y("route_id:N", sort="-x", title="Route ID"),
            ).properties(height=300),
            use_container_width=True
        )

    st.subheader("🚏 Top Start Stations")
    if df_top_starts.empty:
        st.info("No data.")
    else:
        st.altair_chart(
            alt.Chart(df_top_starts).mark_bar().encode(
                x=alt.X("trips:Q", title="Trips"),
                y=alt.Y("station:N", sort="-x", title="Start Station"),
            ).properties(height=300),
            use_container_width=True
        )

    st.subheader("📅 Trips by Month")
    if df_monthly.empty:
        st.info("No data.")
    else:
        st.altair_chart(
            alt.Chart(df_monthly)
            .mark_bar()
            .encode(
                x=alt.X("month:N", sort=month_order, title="Month"),
                y=alt.Y("trips:Q", title="Trips")
            )
            .properties(height=320),
            use_container_width=True
        )

    st.subheader("🌤️ Trips by Season")
    if df_season.empty:
        st.info("No data.")
    else:
        st.altair_chart(
            alt.Chart(df_season).mark_bar().encode(
                x=alt.X("season:N", title="Season"),
                y=alt.Y("trips:Q", title="Trips"),
                color=alt.Color("route_type:N", title=None)
            ).properties(height=320),
            use_container_width=True
        )

    st.markdown(f"### 🧮 Total Trips in {YEAR}: **{total_trips:,}**")
    st.stop()

# ----------------------- PAGE: Review_BB -----------------------
elif page == "Review_BB":
    st.header("📋 Bluebikes — System Review (Historical)")
    st.caption("Source: bluebikes_historical.JantoSep_historical")

    YEAR = 2025
    params_year = [bigquery.ScalarQueryParameter("yy", "INT64", YEAR)]

    sql_total = f"""
        SELECT COUNT(1) AS total_rides
        FROM `{T_BB_HIST}`
        WHERE EXTRACT(YEAR FROM started_at) = @yy
    """
    total_rides = int(run_query(sql_total, params_year, cache_key=f"rev_total::{YEAR}").iloc[0]["total_rides"])

    sql_top_start = f"""
        SELECT start_station_name AS station, COUNT(1) AS trips
        FROM `{T_BB_HIST}`
        WHERE EXTRACT(YEAR FROM started_at) = @yy
        GROUP BY station ORDER BY trips DESC LIMIT 10
    """
    sql_top_end = f"""
        SELECT end_station_name AS station, COUNT(1) AS trips
        FROM `{T_BB_HIST}`
        WHERE EXTRACT(YEAR FROM ended_at) = @yy
        GROUP BY station ORDER BY trips DESC LIMIT 10
    """
    df_top_start = run_query(sql_top_start, params_year, cache_key=f"rev_start::{YEAR}")
    df_top_end = run_query(sql_top_end, params_year, cache_key=f"rev_end::{YEAR}")

    sql_weekday = f"""
        SELECT
          FORMAT_TIMESTAMP('%A', started_at, 'America/New_York') AS weekday,
          member_casual, rideable_type, COUNT(1) AS trips
        FROM `{T_BB_HIST}`
        WHERE EXTRACT(YEAR FROM started_at) = @yy
        GROUP BY weekday, member_casual, rideable_type
    """
    df_weekday = run_query(sql_weekday, params_year, cache_key=f"rev_weekday::{YEAR}")
    week_order = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]
    if not df_weekday.empty:
        df_weekday["weekday"] = pd.Categorical(df_weekday["weekday"], categories=week_order, ordered=True)

    sql_season = f"""
        SELECT
          CASE
            WHEN EXTRACT(MONTH FROM started_at) IN (12,1,2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM started_at) IN (3,4,5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM started_at) IN (6,7,8) THEN 'Summer'
            ELSE 'Fall' END AS season,
          rideable_type, COUNT(1) AS trips
        FROM `{T_BB_HIST}`
        WHERE EXTRACT(YEAR FROM started_at) = @yy
        GROUP BY season, rideable_type
    """
    df_season = run_query(sql_season, params_year, cache_key=f"rev_season::{YEAR}")
    if not df_season.empty:
        df_season["season"] = pd.Categorical(df_season["season"], categories=["Fall","Winter","Spring","Summer"], ordered=True)

    st.subheader("🚲 Start Station Top 10")
    if df_top_start.empty:
        st.info("No data.")
    else:
        st.altair_chart(
            alt.Chart(df_top_start).mark_bar().encode(
                x=alt.X("trips:Q", title="Trips"),
                y=alt.Y("station:N", sort="-x", title="Start Station Name"),
            ).properties(height=300),
            use_container_width=True
        )

    st.subheader("🅿️ End Station Top 10")
    if df_top_end.empty:
        st.info("No data.")
    else:
        st.altair_chart(
            alt.Chart(df_top_end).mark_bar().encode(
                x=alt.X("trips:Q", title="Trips"),
                y=alt.Y("station:N", sort="-x", title="End Station Name"),
            ).properties(height=300),
            use_container_width=True
        )

    st.subheader("📆 Week Day Analysis")
    if df_weekday.empty:
        st.info("No data.")
    else:
        base = (
            alt.Chart(df_weekday)
            .mark_bar()
            .encode(
                x=alt.X("weekday:N", sort=week_order, title=None,
                        axis=alt.Axis(labelAngle=0, labelPadding=8, labelLimit=0)),
                y=alt.Y("trips:Q", title="Total Trips"),
                color=alt.Color("rideable_type:N", title="Rideable Type"),
            )
            .properties(height=360, width=900)
        )
        chart_week = (
            base
            .facet(
                row=alt.Row("member_casual:N", title=None,
                            header=alt.Header(labelFontSize=16, labelPadding=10)),
                columns=1
            )
            .resolve_scale(y="independent")
            .configure_facet(spacing=60)
            .configure_axis(labelFontSize=12, titleFontSize=14)
        )
        st.altair_chart(chart_week, use_container_width=True)

    st.subheader("🌤️ Ride by Season")
    if df_season.empty:
        st.info("No data.")
    else:
        st.altair_chart(
            alt.Chart(df_season).mark_bar().encode(
                x=alt.X("season:N", title="Season"),
                y=alt.Y("trips:Q", title="Total Rides"),
                color=alt.Color("rideable_type:N", title=None),
            ).properties(height=320),
            use_container_width=True
        )

    st.markdown(f"### 🧮 Total Rides in {YEAR}: **{total_rides:,}**")
    st.stop()

# ----------------------- PAGE: Bluebikes Demand Prediction -----------------------
elif page == "Bluebikes Demand Prediction":
    st.header("🤖 Predict demand patterns (pickups / dropoffs, from golden tables)")

    SQL_TOP_PICKS = f"""
      SELECT
        CAST(station_id AS STRING) AS station_id,
        ANY_VALUE(station_name) AS station_name,
        SUM(pickups) AS pred_pickups
      FROM `{T_PRED_PICKUPS}`
      GROUP BY station_id
      ORDER BY pred_pickups DESC
      LIMIT 30
    """

    SQL_TOP_DROPS = f"""
      SELECT
        CAST(station_id AS STRING) AS station_id,
        ANY_VALUE(station_name) AS station_name,
        SUM(dropoffs) AS pred_dropoffs
      FROM `{T_PRED_DROPS}`
      GROUP BY station_id
      ORDER BY pred_dropoffs DESC
      LIMIT 30
    """

    with st.spinner("Loading golden demand tables…"):
        df_top_picks = run_query(SQL_TOP_PICKS, cache_key="golden::top_picks_with_names")
        df_top_drops = run_query(SQL_TOP_DROPS, cache_key="golden::top_drops_with_names")

    if not df_top_picks.empty and not df_top_drops.empty:
        df_common = (
            df_top_picks[["station_id", "station_name", "pred_pickups"]]
            .rename(columns={"pred_pickups": "total_pick"})
            .merge(
                df_top_drops[["station_id", "pred_dropoffs"]]
                .rename(columns={"pred_dropoffs": "total_drop"}),
                on="station_id",
                how="inner",
            )
        )
        df_common["combined_score"] = df_common["total_pick"] + df_common["total_drop"]
        df_common = df_common.sort_values("combined_score", ascending=False).head(20)
    else:
        df_common = pd.DataFrame()

    c1, c2 = st.columns(2, gap="large")

    with c1:
        st.subheader("🏆 Top pickup stations (golden)")
        if df_top_picks.empty:
            st.info("No pickup data from golden table.")
        else:
            show_picks = df_top_picks[["station_id", "station_name", "pred_pickups"]].head(15)
            st.dataframe(show_picks, use_container_width=True)
            st.altair_chart(
                alt.Chart(show_picks).mark_bar().encode(
                    x=alt.X("pred_pickups:Q", title="Total pickups (golden)"),
                    y=alt.Y("station_name:N", sort="-x", title=None),
                ).properties(height=360),
                use_container_width=True,
            )

    with c2:
        st.subheader("🏁 Top dropoff stations (golden)")
        if df_top_drops.empty:
            st.info("No dropoff data from golden table.")
        else:
            show_drops = df_top_drops[["station_id", "station_name", "pred_dropoffs"]].head(15)
            st.dataframe(show_drops, use_container_width=True)
            st.altair_chart(
                alt.Chart(show_drops).mark_bar().encode(
                    x=alt.X("pred_dropoffs:Q", title="Total dropoffs (golden)"),
                    y=alt.Y("station_name:N", sort="-x", title=None),
                ).properties(height=360),
                use_container_width=True,
            )

    st.markdown("---")
    st.subheader("🔗 Stations ranked high in both pickup & dropoff")

    if df_common.empty:
        st.info("No station appears in both top pickup and top dropoff lists.")
    else:
        st.dataframe(
            df_common[["station_id", "station_name", "total_pick", "total_drop", "combined_score"]],
            use_container_width=True,
        )

# ----------------------- PAGE: Bluebikes Clustering -----------------------
if page == "Bluebikes Clustering":
    st.header("🤖 ML Clustering Analysis — Transit Desert Identification")
    st.caption("Using unsupervised K-means clustering to identify MBTA stations with poor last-mile connectivity")
    
    # Load clustering data - get most recent run
    SQL_CLUSTERS = f"""
    WITH latest_run AS (
      SELECT run_id
      FROM `{T_CLUSTERS}`
      ORDER BY run_id DESC
      LIMIT 1
    )
    SELECT DISTINCT
      c.station_id,
      c.station_name,
      c.lat,
      c.lng,
      c.cluster,
      c.cluster_label,
      c.distance_to_nearest_bluebikes_m,
      c.avg_bikes_available_morning,
      c.avg_bikes_available_evening
    FROM `{T_CLUSTERS}` c
    INNER JOIN latest_run lr ON c.run_id = lr.run_id
    """
    
    with st.spinner("Loading clustering results..."):
        df_clusters = run_query(SQL_CLUSTERS, cache_key="ml_clustering_v1")
    
    if df_clusters.empty:
        st.warning("No clustering results found. Please run the clustering DAG first.")
        st.stop()

    # -------------------------------------------------------
    cluster_mean_dist = (
        df_clusters
        .groupby('cluster')['distance_to_nearest_bluebikes_m']
        .mean()
        .sort_values(ascending=False)
    )

    ordered_clusters = cluster_mean_dist.index.tolist()

    label_map = {}
    if len(ordered_clusters) >= 1:
        label_map[ordered_clusters[0]] = 'Transit Desert'
    if len(ordered_clusters) >= 2:
        label_map[ordered_clusters[1]] = 'Moderate Coverage'
    if len(ordered_clusters) >= 3:
        label_map[ordered_clusters[2]] = 'Well-Served'

    df_clusters['cluster_label'] = df_clusters['cluster'].map(label_map)
    
    # ----------------------- EXECUTIVE SUMMARY -----------------------
    st.subheader("📊 Executive Summary")
    
    total_stations = len(df_clusters)
    transit_deserts = len(df_clusters[df_clusters['cluster_label'] == 'Transit Desert'])
    well_served = len(df_clusters[df_clusters['cluster_label'] == 'Well-Served'])
    moderate = len(df_clusters[df_clusters['cluster_label'] == 'Moderate Coverage'])
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total MBTA Stations", total_stations)
    with col2:
        st.metric(
            "🔴 Transit Deserts",
            transit_deserts,
            delta=f"{transit_deserts/total_stations*100:.1f}%",
            delta_color="inverse"
        )
    with col3:
        st.metric(
            "🟢 Well-Served",
            well_served,
            delta=f"{well_served/total_stations*100:.1f}%"
        )
    with col4:
        st.metric(
            "🟡 Moderate Coverage",
            moderate,
            delta=f"{moderate/total_stations*100:.1f}%"
        )
    
    st.markdown("---")
    
    # ----------------------- CLUSTER CHARACTERISTICS -----------------------
    st.subheader("📈 Cluster Characteristics")
    
    cluster_stats = df_clusters.groupby('cluster_label').agg({
        'station_id': 'count',
        'distance_to_nearest_bluebikes_m': 'mean',
        'avg_bikes_available_morning': 'mean',
        'avg_bikes_available_evening': 'mean'
    }).round(1)
    
    cluster_stats.columns = ['Station Count', 'Avg Distance (m)', 'Avg Morning Bikes', 'Avg Evening Bikes']
    cluster_stats = cluster_stats.reset_index()

    st.dataframe(
        cluster_stats.style.format({
            'Station Count': '{:.0f}',
            'Avg Distance (m)': '{:.0f}',
            'Avg Morning Bikes': '{:.1f}',
            'Avg Evening Bikes': '{:.1f}'
        }),
        use_container_width=True
    )
    
    # Visualization of cluster distribution
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Distribution by Cluster Type**")
        cluster_dist = df_clusters['cluster_label'].value_counts().reset_index()
        cluster_dist.columns = ['cluster_label', 'count']
        
        chart = alt.Chart(cluster_dist).mark_bar().encode(
            x=alt.X('count:Q', title='Number of Stations'),
            y=alt.Y('cluster_label:N', title='Cluster Type',
                    sort=['Transit Desert', 'Moderate Coverage', 'Well-Served']),
            color=alt.Color(
                'cluster_label:N',
                scale=alt.Scale(
                    domain=['Transit Desert', 'Moderate Coverage', 'Well-Served'],
                    range=['#d62728', '#ff7f0e', '#2ca02c']
                ),
                legend=None
            )
        ).properties(height=200)
        st.altair_chart(chart, use_container_width=True)
    
    with col2:
        st.markdown("**Average Distance to Nearest Bluebikes**")
        avg_dist = df_clusters.groupby('cluster_label')['distance_to_nearest_bluebikes_m'].mean().reset_index()
        
        chart = alt.Chart(avg_dist).mark_bar().encode(
            x=alt.X('distance_to_nearest_bluebikes_m:Q', title='Distance (meters)'),
            y=alt.Y('cluster_label:N', title='Cluster Type',
                    sort=['Transit Desert', 'Moderate Coverage', 'Well-Served']),
            color=alt.Color(
                'cluster_label:N',
                scale=alt.Scale(
                    domain=['Transit Desert', 'Moderate Coverage', 'Well-Served'],
                    range=['#d62728', '#ff7f0e', '#2ca02c']
                ),
                legend=None
            )
        ).properties(height=200)
        st.altair_chart(chart, use_container_width=True)
    
    st.markdown("---")
    
    # ----------------------- INTERACTIVE MAP -----------------------
    st.subheader("🗺️ Interactive Cluster Map")
    st.caption("Click on stations to see details. Color indicates cluster type.")
    
    df_map = df_clusters.copy()
    
    color_map = {
        'Transit Desert': [214, 39, 40, 200],     
        'Moderate Coverage': [255, 127, 14, 200],  
        'Well-Served': [44, 160, 44, 200]         
    }
    df_map['color'] = df_map['cluster_label'].map(color_map)
    
    cluster_filter = st.multiselect(
        "Filter by cluster type:",
        options=['Transit Desert', 'Moderate Coverage', 'Well-Served'],
        default=['Transit Desert', 'Moderate Coverage', 'Well-Served']
    )
    
    df_map_filtered = df_map[df_map['cluster_label'].isin(cluster_filter)]
    
    view_state = pdk.ViewState(
        latitude=df_map_filtered['lat'].mean(),
        longitude=df_map_filtered['lng'].mean(),
        zoom=10,
        pitch=0
    )
    
    layer = pdk.Layer(
        'ScatterplotLayer',
        data=df_map_filtered,
        get_position='[lng, lat]',
        get_color='color',
        get_radius=100,
        pickable=True,
        auto_highlight=True
    )
    
    tooltip = {
        "html": "<b>{station_name}</b><br/>"
                "Type: {cluster_label}<br/>"
                "Distance to Bluebikes: {distance_to_nearest_bluebikes_m:.0f}m<br/>"
                "Morning bikes: {avg_bikes_available_morning:.1f}<br/>"
                "Evening bikes: {avg_bikes_available_evening:.1f}",
        "style": {"backgroundColor": "steelblue", "color": "white"}
    }
    
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style='mapbox://styles/mapbox/light-v10'
    )
    
    st.pydeck_chart(deck, use_container_width=True)
    
    st.markdown("---")
    
    # ----------------------- DETAILED TABLES -----------------------
    st.subheader("📋 Detailed Station Lists")
    
    tab1, tab2, tab3 = st.tabs(["🔴 Transit Deserts", "🟡 Moderate Coverage", "🟢 Well-Served"])
    
    with tab1:
        st.markdown("### Transit Desert Stations")
        st.caption("These stations have poor last-mile connectivity and should be prioritized for improvement.")
        
        transit_desert_df = df_clusters[df_clusters['cluster_label'] == 'Transit Desert'].copy()
        transit_desert_df = transit_desert_df.sort_values('distance_to_nearest_bluebikes_m', ascending=False)
        
        st.dataframe(
            transit_desert_df[[
                'station_name', 
                'distance_to_nearest_bluebikes_m', 
                'avg_bikes_available_morning',
                'avg_bikes_available_evening'
            ]].rename(columns={
                'station_name': 'Station Name',
                'distance_to_nearest_bluebikes_m': 'Distance to Bluebikes (m)',
                'avg_bikes_available_morning': 'Morning Bikes Available',
                'avg_bikes_available_evening': 'Evening Bikes Available'
            }).style.format({
                'Distance to Bluebikes (m)': '{:.0f}',
                'Morning Bikes Available': '{:.1f}',
                'Evening Bikes Available': '{:.1f}'
            }),
            use_container_width=True,
            height=400
        )
        
        st.markdown("**Top 10 Most Isolated Stations**")
        top_10_worst = transit_desert_df.nlargest(10, 'distance_to_nearest_bluebikes_m')
        
        chart = alt.Chart(top_10_worst).mark_bar(color='#d62728').encode(
            y=alt.Y('station_name:N', sort='-x', title=None),
            x=alt.X('distance_to_nearest_bluebikes_m:Q', title='Distance to Nearest Bluebikes (meters)'),
            tooltip=['station_name', 'distance_to_nearest_bluebikes_m']
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)
    
    with tab2:
        st.markdown("### Moderate Coverage Stations")
        st.caption("These stations have decent but not ideal connectivity.")
        
        moderate_df = df_clusters[df_clusters['cluster_label'] == 'Moderate Coverage'].copy()
        moderate_df = moderate_df.sort_values('distance_to_nearest_bluebikes_m', ascending=False)
        
        st.dataframe(
            moderate_df[[
                'station_name', 
                'distance_to_nearest_bluebikes_m', 
                'avg_bikes_available_morning',
                'avg_bikes_available_evening'
            ]].rename(columns={
                'station_name': 'Station Name',
                'distance_to_nearest_bluebikes_m': 'Distance to Bluebikes (m)',
                'avg_bikes_available_morning': 'Morning Bikes Available',
                'avg_bikes_available_evening': 'Evening Bikes Available'
            }).style.format({
                'Distance to Bluebikes (m)': '{:.0f}',
                'Morning Bikes Available': '{:.1f}',
                'Evening Bikes Available': '{:.1f}'
            }),
            use_container_width=True,
            height=400
        )
    
    with tab3:
        st.markdown("### Well-Served Stations")
        st.caption("These stations have excellent last-mile connectivity.")
        
        well_served_df = df_clusters[df_clusters['cluster_label'] == 'Well-Served'].copy()
        well_served_df = well_served_df.sort_values('avg_bikes_available_morning', ascending=False)
        
        st.dataframe(
            well_served_df[[
                'station_name', 
                'distance_to_nearest_bluebikes_m', 
                'avg_bikes_available_morning',
                'avg_bikes_available_evening'
            ]].rename(columns={
                'station_name': 'Station Name',
                'distance_to_nearest_bluebikes_m': 'Distance to Bluebikes (m)',
                'avg_bikes_available_morning': 'Morning Bikes Available',
                'avg_bikes_available_evening': 'Evening Bikes Available'
            }).style.format({
                'Distance to Bluebikes (m)': '{:.0f}',
                'Morning Bikes Available': '{:.1f}',
                'Evening Bikes Available': '{:.1f}'
            }),
            use_container_width=True,
            height=400
        )
    
    st.markdown("---")

# ----------------------- PAGE: Availability -----------------------
elif page == "Availability":
    st.header("🔎 Availability — Nearest Bluebikes for a Selected MBTA Station")

    with st.form("availability_search_form", clear_on_submit=False):
        station_keyword = st.text_input(
            "Enter MBTA station keyword (e.g., 'Quincy', 'Forest Hills', 'Washington')",
            key="station_kw",
            help="Type a keyword and press Enter or click Search",
        )
        submitted = st.form_submit_button("Search")

    if "matched_stations" not in st.session_state:
        st.session_state["matched_stations"] = None
    if "last_keyword" not in st.session_state:
        st.session_state["last_keyword"] = ""

    if station_keyword != st.session_state["last_keyword"]:
        st.session_state["matched_stations"] = None

    if submitted:
        if not station_keyword.strip():
            st.warning("Please enter a station keyword first.")
            st.stop()

        with st.spinner("Searching MBTA stations matching your keyword…"):
            search_sql = f"""
                SELECT station_id, station_name
                FROM `{T_MBTA_STATIONS}`
                WHERE LOWER(station_name) LIKE LOWER(CONCAT('%', @kw, '%'))
                ORDER BY station_name
            """
            stations_df = run_query(
                search_sql,
                [bigquery.ScalarQueryParameter("kw", "STRING", station_keyword)],
                cache_key=f"mbta_search::{station_keyword}"
            )

        st.session_state["last_keyword"] = station_keyword
        st.session_state["matched_stations"] = stations_df

    stations_df = st.session_state.get("matched_stations")

    if stations_df is None:
        st.info("Search for an MBTA station above to see availability.")
        st.stop()

    if stations_df.empty:
        st.warning("No matching MBTA stations found. Try a different keyword.")
        st.stop()

    st.markdown("### Select your MBTA station")

    options = {
        f"{row['station_name']} (ID: {row['station_id']})": str(row["station_id"])
        for _, row in stations_df.iterrows()
    }

    selected_label = st.selectbox(
        "Choose one station from the search results",
        list(options.keys())
    )
    chosen_id = options[selected_label]

    with st.spinner("Finding nearest Bluebikes station and current status…"):
        qparams = [bigquery.ScalarQueryParameter("mbta_station_id", "STRING", chosen_id)]
        nearest_df = run_query(SQL_NEAREST_AVAIL, qparams, cache_key=f"nearest::{chosen_id}")

    if nearest_df.empty:
        st.info("No nearby Bluebikes station found.")
        st.stop()

    row = nearest_df.iloc[0]

    st.markdown(f"### 🚉 MBTA Station: **{row['mbta_station_name']}**")
    st.markdown(f"### 🚲 Nearest Bluebikes Station: **{row['bb_name']}**")
    st.markdown("#### Current Bluebikes Availability and Status")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Distance (m)", f"{row['distance_m']:.0f}")
    c2.metric("Status Time (UTC)", str(row.get("status_snapshot_ts", ""))[:19])

    renting_val = str(row.get("is_renting"))
    returning_val = str(row.get("is_returning"))
    renting_ok = renting_val in ("1", "True", "true")
    returning_ok = returning_val in ("1", "True", "true")

    c3.metric("Is Renting", "✅" if renting_ok else "❌")
    c4.metric("Is Returning", "✅" if returning_ok else "❌")

    c5, c6 = st.columns(2)
    c5.metric("Bikes Available", int(row.get("num_bikes_available") or 0))
    c6.metric("Docks Available", int(row.get("num_docks_available") or 0))

    mdf = pd.DataFrame([
        {
            "name": f"MBTA — {row['mbta_station_name']}",
            "lat": row["mbta_lat"],
            "lon": row["mbta_lng"],
            "color": [0, 102, 204, 220],
        },
        {
            "name": f"Bluebikes — {row['bb_name']}",
            "lat": row["bb_lat"],
            "lon": row["bb_lng"],
            "color": [0, 200, 100, 220],
        },
    ])
    line_df = pd.DataFrame([{
        "from_lon": row["mbta_lng"], "from_lat": row["mbta_lat"],
        "to_lon": row["bb_lng"], "to_lat": row["bb_lat"],
    }])

    view = pdk.ViewState(
        latitude=mdf["lat"].mean(),
        longitude=mdf["lon"].mean(),
        zoom=12
    )

    layer_points = pdk.Layer(
        "ScatterplotLayer",
        data=mdf,
        get_position='[lon, lat]',
        get_fill_color='color',
        get_radius=70,
        pickable=True
    )
    layer_line = pdk.Layer(
        "LineLayer",
        data=line_df,
        get_source_position='[from_lon, from_lat]',
        get_target_position='[to_lon, to_lat]',
        get_width=4,
        get_color=[255, 140, 0]
    )

    st.pydeck_chart(pdk.Deck(
        initial_view_state=view,
        layers=[layer_points, layer_line],
        tooltip={"text": "{name}"}
    ))

# ----------------------- PAGE: Bluebikes Assistant (LLM) -----------------------
elif page == "Bluebikes & Policy Assistant":
    st.markdown(
        """
        <h2 style='text-align:center; margin-bottom: 0.2rem;'>
            🤖 Unified Assistant (Bluebikes + Policy)
        </h2>
        <p style='text-align:center; opacity:0.7;'>
            Ask anything about Bluebikes usage, availability, or policies.
            The system will automatically route your question to the right agent.
        </p>
        """,
        unsafe_allow_html=True
    )

    if "unified_history" not in st.session_state:
        st.session_state.unified_history = []

    default_q = "Can I bring a Bluebikes bike onto the MBTA subway?"

    st.markdown("### 💬 Your question:")
    user_q = st.text_area(
        "",
        value=default_q,
        height=80,
        key="unified_question",
    )

    col1, col2 = st.columns([1, 3])

    with col1:
        if st.button("🗑️ Clear History", use_container_width=True, key="unified_clear"):
            st.session_state.unified_history = []
            st.experimental_rerun()

        if st.button("🔍 Ask Unified Agent", use_container_width=True, key="unified_ask"):
            q = user_q.strip()
            if q:
                with st.spinner("Routing to the right agent and generating answer..."):
                    try:
                        ans = ask_unified_agent(q)
                    except Exception as e:
                        ans = f"Unified agent error: {e}"
                st.session_state.unified_history.append({"q": q, "a": ans})

    with col2:
        st.subheader("🧾 Conversation")
        if not st.session_state.unified_history:
            st.info("No questions yet.")
        else:
            for item in reversed(st.session_state.unified_history):
                st.markdown(
                    f"""
                    <div style="margin-bottom: 1rem; padding: 0.8rem; border-radius: 8px;
                                background-color: rgba(255,255,255,0.05);">
                        <div><b>🧑 You:</b><br>{item['q']}</div>
                        <hr style="margin: 0.5rem 0;">
                        <div><b>🤖 Unified Assistant:</b><br>{item['a']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
# ----------------------- PAGE: AI Transit Assistant -----------------------
elif page == "AI Transit Assistant":
    st.header("🤖 AI Transit Assistant (MBTA + ML + RAG)")
    st.caption("Ask things like: \"I'm at Primark on Park Street and want to go to Chiswick Road\"")

    user_q = st.text_input("Your question", "")

    if user_q.strip():
        with st.spinner("Understanding your query…"):
            origin, dest = extract_origin_destination(user_q)
            st.write(f"**Debug:** origin = `{origin}`, dest = `{dest}`")

        if not origin or not dest:
            st.error("I couldn't detect an origin and destination. Try rephrasing.")
            st.stop()

        def clean_place(p):
            if not p:
                return None
            p = p.strip()
            p = p.replace(",", "")
            p = p.replace("station", "")
            p = p.replace("St.", "Street")
            p = p.replace("st.", "street")
            p = p.replace("road", "Road")
            return " ".join(p.split()).title()

        origin_clean = clean_place(origin)
        dest_clean = clean_place(dest)

        o_lat, o_lon = geocode_place(origin_clean)
        d_lat, d_lon = geocode_place(dest_clean)

        if o_lat is None or d_lat is None:
            st.error("Couldn't geocode one of the locations.")
            st.stop()

        o_station_id, o_station_name = find_nearest_station(o_lat, o_lon)
        d_station_id, d_station_name = find_nearest_station(d_lat, d_lon)

        if not o_station_id or not d_station_id:
            st.error("Nearby MBTA stations not found.")
            st.stop()

        lat_bin = round(o_lat, 1)
        lon_bin = round(o_lon, 1)
        SQL_OCC = f"""
            SELECT predicted_label
            FROM ML.PREDICT(MODEL `{PROJECT_ID}.mbta_ml.occ_lr_min`,
              (SELECT
                  'Red' AS route_id,
                  'IN_TRANSIT' AS current_status,
                  8 AS hour_of_day,
                  2 AS day_of_week,
                  0 AS is_weekend,
                  {lat_bin} AS lat_bin,
                  {lon_bin} AS lon_bin
              ))
        """
        try:
            occ_df = run_query(SQL_OCC, cache_key=f"ai_occ::{o_station_id}")
            occupancy = occ_df.iloc[0]["predicted_label"] if not occ_df.empty else "Unknown"
        except Exception:
            occupancy = "Unknown"

        departures = get_live_departures(o_station_id)
        rag_ctx = rag_retrieve_context(f"{origin} {dest} {o_station_name} {d_station_name}")

        answer = generate_final_answer(
            user_q, origin, dest,
            o_station_name, d_station_name,
            occupancy, departures, rag_ctx
        )

        st.markdown("### 🧭 Route Recommendation")
        st.write(answer)
