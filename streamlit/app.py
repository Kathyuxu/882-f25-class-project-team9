# app.py — MBTA × Bluebikes "Last-Mile"
# Pages: Overview / Recommendations / Availability (keyword search)
# - LEFT JOIN shows MBTA stations even if no Bluebikes nearby
# - Streamlit cache param fix (underscore)
# - Availability: text keyword input -> parameterized LIKE + form submit
# - Availability layout: line 1 MBTA station, line 2 nearest Bluebikes, line 3 metrics

import os
import altair as alt
import pandas as pd
import streamlit as st
import pydeck as pdk
from google.cloud import bigquery
from google import genai
import json
import requests


st.set_page_config(page_title="MBTA × Bluebikes — Last-Mile", layout="wide")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ba882-f25-class-project-team9")
T_MBTA_STATIONS = f"{PROJECT_ID}.mbta_data.stations_dedup"
T_BB_INFO      = f"{PROJECT_ID}.bluebikes_analysis.station_info"
T_BB_STATUS    = f"{PROJECT_ID}.bluebikes_analysis.station_status"

client = bigquery.Client(project=PROJECT_ID)
# === AI + Geocoder + API ===
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
    # You can expand this later
}


# ---------- Cached Query ----------
@st.cache_data(ttl=600)
def run_query(sql: str, _params=None, cache_key: str = "") -> pd.DataFrame:
    # cache_key
    _ = cache_key
    job_config = bigquery.QueryJobConfig(query_parameters=_params or [])
    return client.query(sql, job_config=job_config).to_dataframe()

# ---------- Sidebar ----------
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
        "Summary",
        "Overview",
        "Review_MBTA",
        "Recommendations",
        "Availability",
        "Occupancy Prediction",
        "AI Transit Assistant"
    ]
)


# ---------- SQL (LEFT JOIN so MBTA stations without Bluebikes remain) ----------
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

# ---------- Availability Page SQLs ----------
SQL_MBTA_LIST = f"""
SELECT station_id, station_name
FROM `{T_MBTA_STATIONS}`
ORDER BY station_name
"""

# No 'status' column; keep latest snapshot fields that exist
SQL_NEAREST_AVAIL = f"""
WITH m AS (
  SELECT
    station_id,
    station_name,
    CAST(lat AS FLOAT64) AS lat,
    CAST(lng AS FLOAT64) AS lng,
    ST_GEOGPOINT(CAST(lng AS FLOAT64), CAST(lat AS FLOAT64)) AS geog
  FROM `{T_MBTA_STATIONS}`
  WHERE station_id = @mbta_station_id  -- STRING param
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

# ---------- Run main query for Overview/Recommendations ----------
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

# ---------- Helper for map ----------
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

# ------------------------- AI Helper Functions -------------------------




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

    df = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("name", "STRING", place_text)
            ]
        )
    ).to_dataframe()

    if df.empty:
        print("❌ NO MATCH FOR:", place_text)
        return None, None

    print("✅ MATCH FOUND:", df.iloc[0]["station_name"])
    return df.iloc[0]["lat"], df.iloc[0]["lng"]


def find_nearest_station(lat: float, lon: float):
    """Find nearest MBTA station using stations_dedup."""
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
    df = client.query(q).to_dataframe()
    if df.empty:
        return None, None
    row = df.iloc[0]
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

    # --- cleanup common issues ---
    raw = raw.replace("```json", "").replace("```", "").strip()

    import json, re

    # Try parsing raw JSON directly
    try:
        data = json.loads(raw)
        return data.get("origin"), data.get("destination")
    except:
        pass

    # Fallback: extract JSON inside longer text
    try:
        match = re.search(r"{.*}", raw, re.DOTALL)
        if match:
            data = json.loads(match.group(0))
            return data.get("origin"), data.get("destination")
    except:
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

    df = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("emb", "FLOAT64", emb),
                bigquery.ScalarQueryParameter("top_k", "INT64", top_k)
            ]
        )
    ).to_dataframe()

    return "\n".join(df["text"]) if not df.empty else ""


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
    except:
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




# ---------- Classify failing stations (for recommendation bullets) ----------
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
# ---------- Descriptive Summary ----------
if page == "Summary":
    st.header("📊 Descriptive Summary")

    # 核心汇总
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

    # 顶部指标卡
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total MBTA Stations (filtered)", total_stations)
    c2.metric("Stations Meeting Rule", num_meets)
    c3.metric("Stations NOT Meeting Rule", num_fails)
    c4.metric("Compliance Rate", f"{percent_meets:.1f}%")

    # 补充要点
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

    # 近/远 Top 10（可选）
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

    st.stop()  # 结束本页渲染，避免继续渲染其他页面分支

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

    # Two maps
    render_map(meets, [0, 180, 0, 200], "🗺️ Map — Stations meeting the rule")
    render_map(fails, [200, 0, 0, 220], "🗺️ Map — Stations NOT meeting the rule")

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
  - For **Far-end / Park & Ride** stations (e.g., Newburyport, Littleton/Route 495), roll out **Park-and-Bike** and evaluate ridership before adding docks.
  - Expand to regional hubs (e.g., **Lawrence**, **New Bedford**) when infrastructure is ready.
- **Long-term (24+ months)**
  - Deploy **seasonal docks** in tourism corridors (e.g., **Greenbush**, **Gloucester**) for Apr–Oct.
  - Build a **multi-modal** network with micromobility (e-scooters, carshare) for suburban coverage.
""")

    st.markdown("#### Map of Non-Compliant MBTA Stations")
    render_map(fails, [200, 0, 0, 220], "🗺️ MBTA Stations Not Meeting Last-Mile Rule")

    with st.expander("See list of failing stations"):
        st.dataframe(fails[["station_id", "station_name"]], use_container_width=True)
# ----------------------- PAGE: Occupancy Prediction -----------------------
# ----------------------- PAGE: Occupancy Prediction -----------------------
elif page == "Occupancy Prediction":
    st.header("🚌 MBTA Vehicle Occupancy Prediction (via BigQuery ML)")
    st.caption("Predict real-time train/bus occupancy levels using the deployed Logistic Regression model.")

    # --- Load all distinct route IDs dynamically ---
    try:
        SQL_ROUTE_IDS = f"SELECT DISTINCT route_id FROM `{PROJECT_ID}.mbta_data.mbta_historical` ORDER BY route_id"
        route_df = run_query(SQL_ROUTE_IDS, cache_key="route_ids")
        route_options = sorted(route_df["route_id"].dropna().unique().tolist())
    except Exception as e:
        st.error(f"Error fetching route list: {e}")
        route_options = ["Red", "Green", "Orange", "Blue", "1", "47"]

    # --- User Inputs ---
    route_id = st.selectbox("🚏 Select Route ID", route_options, index=0)
    direction_id = st.selectbox("🧭 Direction", [0, 1])
    hour_of_day = st.slider("🕒 Hour of Day", 0, 23, 8)

    day_map = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6
    }
    day_of_week_label = st.selectbox("📅 Day of Week", list(day_map.keys()))
    day_of_week = day_map[day_of_week_label]

    current_status = st.selectbox("🚦 Current Status", ["IN_TRANSIT", "STOPPED_AT", "INCOMING_AT"])

    # --- Step 1: Auto-fetch route coordinates ---
    SQL_FETCH_COORDS = f"""
    SELECT
      AVG((start_lat + end_lat) / 2.0) AS lat,
      AVG((start_lng + end_lng) / 2.0) AS lon
    FROM `{PROJECT_ID}.mbta_data.mbta_historical`
    WHERE LOWER(route_id) = LOWER('{route_id}')
    """
    try:
        coords = run_query(SQL_FETCH_COORDS, cache_key=f"coords::{route_id}")
        if not coords.empty and not coords.isnull().values.any():
            lat = coords.iloc[0]["lat"]
            lon = coords.iloc[0]["lon"]
            lat_bin, lon_bin = float(round(lat, 1)), float(round(lon, 1))
            st.info(f"📍 Coordinates fetched: **({lat:.4f}, {lon:.4f}) → bins ({lat_bin}, {lon_bin})**")
        else:
            st.warning(f"No coordinates found for route '{route_id}'. Using default bins.")
            lat_bin, lon_bin = 42.3, -71.1
    except Exception as e:
        st.error(f"Error fetching coordinates: {e}")
        lat_bin, lon_bin = 42.3, -71.1

    # --- Step 2: BigQuery ML Prediction ---
    SQL_OCC_PRED = f"""
    SELECT predicted_label, predicted_label_probs
    FROM ML.PREDICT(MODEL `{PROJECT_ID}.mbta_ml.occ_lr_min`,
      (SELECT
          '{route_id}' AS route_id,
          '{current_status}' AS current_status,
          {hour_of_day} AS hour_of_day,
          {day_of_week} AS day_of_week,
          {1 if day_of_week in [5, 6] else 0} AS is_weekend,
          {lat_bin} AS lat_bin,
          {lon_bin} AS lon_bin
      ))
    """

    # --- Step 3: Run Prediction ---
    if st.button("👾 Run Prediction"):
        with st.spinner("Querying BigQuery ML model..."):
            df_pred = run_query(
                SQL_OCC_PRED,
                cache_key=f"occ_pred::{route_id}::{day_of_week}"
                )
            if not df_pred.empty:
                pred_label = df_pred.iloc[0]["predicted_label"]
                # Color-coded output
                color_map = {
                    "MANY_SEATS_AVAILABLE": "green",
                    "FEW_SEATS_AVAILABLE": "orange",
                    "FULL": "red",
                    }
                color = color_map.get(pred_label, "gray")
                st.markdown(
                    f"""
                    <div style="
                    background-color:{color};
                    padding:15px;
                    border-radius:8px;
                    color:white;
                    font-size:18px;">
                    🚆 <b>Predicted Occupancy:</b> {pred_label.replace('_', ' ').title()}
                </div>
                """,
                    unsafe_allow_html=True
                    )
            else:
                st.warning("No prediction result returned from BigQuery ML model.")

    # --- 3. Auto-fetch average coordinates for the selected route ---
    SQL_FETCH_COORDS = f"""
    SELECT
      AVG((start_lat + end_lat) / 2.0) AS lat,
      AVG((start_lng + end_lng) / 2.0) AS lon
    FROM `{PROJECT_ID}.mbta_data.mbta_historical`
    WHERE LOWER(route_id) = LOWER('{route_id}')
    """
    try:
        coords = run_query(SQL_FETCH_COORDS, cache_key=f"coords::{route_id}")
        if not coords.empty and not coords.isnull().values.any():
            lat = coords.iloc[0]["lat"]
            lon = coords.iloc[0]["lon"]
            lat_bin, lon_bin = float(round(lat, 1)), float(round(lon, 1))
            st.info(f"📍 Coordinates fetched: ({lat:.4f}, {lon:.4f}) → bins ({lat_bin}, {lon_bin})")
        else:
            st.warning(f"No coordinates found for route '{route_id}'. Using defaults.")
            lat_bin, lon_bin = 42.3, -71.1
    except Exception as e:
        st.error(f"Error fetching coordinates: {e}")
        lat_bin, lon_bin = 42.3, -71.1

    # --- 4. Build BigQuery ML Prediction Query ---
    SQL_OCC_PRED = f"""
    SELECT predicted_label, predicted_label_probs
    FROM ML.PREDICT(MODEL `{PROJECT_ID}.mbta_ml.occ_lr_min`,
      (SELECT
          '{route_id}' AS route_id,
          '{current_status}' AS current_status,
          {hour_of_day} AS hour_of_day,
          {day_of_week} AS day_of_week,
          {1 if day_of_week in [5, 6] else 0} AS is_weekend,
          {lat_bin} AS lat_bin,
          {lon_bin} AS lon_bin
      ))
    """

    # --- 5. Run Prediction Automatically ---
    try:
        with st.spinner("Querying BigQuery ML model..."):
            df_pred = run_query(SQL_OCC_PRED, cache_key=f"occ_pred::{route_id}::{hour_of_day}::{day_of_week}")

        if not df_pred.empty:
            pred_label = df_pred.iloc[0]["predicted_label"]
            probs = df_pred.iloc[0]["predicted_label_probs"]

            # --- 6. Color-coded output for prediction ---
            color_map = {
                "MANY_SEATS_AVAILABLE": "#2ecc71",  # Green
                "FEW_SEATS_AVAILABLE": "#f39c12",   # Orange
                "FULL": "#e74c3c"                   # Red
            }
            color = color_map.get(pred_label, "#7f8c8d")

            st.markdown(
                f"""
                <div style="background-color:{color};padding:18px;border-radius:10px;color:white;font-size:20px;font-weight:600;text-align:center;">
                    🚆 Predicted Occupancy: {pred_label.replace('_', ' ').title()}
                </div>
                """,
                unsafe_allow_html=True
            )

            # --- 7. Display confidence breakdown as chart ---
            if isinstance(probs, list) or isinstance(probs, dict):
                probs_df = pd.DataFrame(probs)
            else:
                probs_df = pd.DataFrame(eval(probs))  # handle stringified dict

            if not probs_df.empty and "label" in probs_df.columns and "prob" in probs_df.columns:
                st.subheader("📊 Model Confidence")
                chart = (
                    alt.Chart(probs_df)
                    .mark_bar(size=40)
                    .encode(
                        x=alt.X("label:N", title="Occupancy Level"),
                        y=alt.Y("prob:Q", title="Probability"),
                        color=alt.Color("label:N", legend=None)
                    )
                    .properties(height=300)
                )
                st.altair_chart(chart, use_container_width=True)
        else:
            st.warning("No prediction result returned from BigQuery ML model.")
    except Exception:
        pass

# ----------------------- PAGE: Review_MBTA -----------------------
elif page == "Review_MBTA":
    st.header("🚆 MBTA — System Review (Historical)")
    st.caption("Source: mbta_data.mbta_historical")

    YEAR = st.selectbox("Select Year", [2023, 2024, 2025], index=2)
    params_year = [bigquery.ScalarQueryParameter("yy", "INT64", YEAR)]

    # ---- Total trips ----
    sql_total = f"""
        SELECT COUNT(1) AS total_trips
        FROM `{PROJECT_ID}.mbta_data.mbta_historical`
        WHERE EXTRACT(YEAR FROM scheduled_start_time) = @yy
    """
    total_trips = int(run_query(sql_total, params_year, cache_key=f"mbta_total::{YEAR}").iloc[0]["total_trips"])

    # ---- Top routes ----
    sql_top_routes = f"""
        SELECT route_id, COUNT(1) AS trips
        FROM `{PROJECT_ID}.mbta_data.mbta_historical`
        WHERE EXTRACT(YEAR FROM scheduled_start_time) = @yy
        GROUP BY route_id ORDER BY trips DESC LIMIT 10
    """
    df_top_routes = run_query(sql_top_routes, params_year, cache_key=f"mbta_routes::{YEAR}")

    # ---- Top start stations ----
    sql_top_starts = f"""
        SELECT start_station_name AS station, COUNT(1) AS trips
        FROM `{PROJECT_ID}.mbta_data.mbta_historical`
        WHERE EXTRACT(YEAR FROM scheduled_start_time) = @yy
        GROUP BY station ORDER BY trips DESC LIMIT 10
    """
    df_top_starts = run_query(sql_top_starts, params_year, cache_key=f"mbta_starts::{YEAR}")

# ---- Month-wise trends ----
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
    # ---- Seasonality ----
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

    # ---- Visuals ----
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
            .mark_bar(color="#2E86DE")
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
# ----------------------- PAGE: AI Transit Assistant -----------------------
elif page == "AI Transit Assistant":
    st.header("🤖 AI Transit Assistant (MBTA + ML + RAG)")
    st.caption("Ask natural questions like: 'I'm at Primark on Park Street and want to go to Chiswick Road'.")

    user_q = st.text_input("Your question", "")

    if user_q.strip():
        with st.spinner("Understanding your query…"):
            origin, dest = extract_origin_destination(user_q)
            print("🔥 RAW ORIGIN FROM LLM:", origin)
            print("🔥 RAW DEST FROM LLM:", dest)
            st.write(f"**Debug:** origin=`{origin}`, dest=`{dest}`")



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



        # Geocode both places
        print("📍 ORIGIN RESULT:", o_lat, o_lon)
        print("📍 deo RESULT:", d_lat, d_lon)



        if o_lat is None or d_lat is None:
            st.error("Couldn't geocode one of the locations.")
            st.stop()

        # Find nearest MBTA stations
        o_station_id, o_station_name = find_nearest_station(o_lat, o_lon)
        d_station_id, d_station_name = find_nearest_station(d_lat, d_lon)

        if not o_station_id or not d_station_id:
            st.error("Nearby MBTA stations not found.")
            st.stop()

        # Occupancy prediction using your existing model’s features
        # (Here we reuse the nearest-station lat/lng → bins)
        lat_bin = round(o_lat, 1)
        lon_bin = round(o_lon, 1)
        SQL_OCC = f"""
            SELECT predicted_label
            FROM ML.PREDICT(MODEL `{PROJECT_ID}.mbta_ml.occ_lr_min`,
              (SELECT
                  'Red' AS route_id,       -- fallback route
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
        except:
            occupancy = "Unknown"

        # Live departures
        departures = get_live_departures(o_station_id)

        # RAG context
        rag_ctx = rag_retrieve_context(f"{origin} {dest} {o_station_name} {d_station_name}")

        # Final AI answer
        answer = generate_final_answer(
            user_q, origin, dest,
            o_station_name, d_station_name,
            occupancy, departures, rag_ctx
        )

        st.markdown("### 🧭 Route Recommendation")
        st.write(answer)


else:  # Availability — keyword search + dropdown
    st.header("🔎 Availability — Nearest Bluebikes for a Selected MBTA Station")

    # --- Search form ---
    with st.form("availability_search_form", clear_on_submit=False):
        station_keyword = st.text_input(
            "Enter MBTA station keyword (e.g., 'Washington', 'Quincy', 'Forest Hills')",
            key="station_kw",
            help="Type a keyword and press Enter or click Search",
        )
        submitted = st.form_submit_button("Search")

    if submitted:
        if not station_keyword.strip():
            st.warning("Please enter a station keyword first.")
            st.stop()

        # Query all matching stations
        with st.spinner("Searching MBTA stations..."):
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

        if stations_df.empty:
            st.warning("No matching MBTA stations found. Try a different keyword.")
            st.stop()

        # Save search results into session
        st.session_state["matched_stations"] = stations_df

    # --- If we have matched stations, show dropdown ---
    if "matched_stations" in st.session_state:
        stations_df = st.session_state["matched_stations"]

        st.markdown("### Select your station")
        station_choices = {
            f"{row['station_name']} (ID: {row['station_id']})": row["station_id"]
            for _, row in stations_df.iterrows()
        }

        selected_label = st.selectbox(
            "Choose your MBTA station",
            list(station_choices.keys())
        )
        selected_station_id = station_choices[selected_label]

        # Query nearest Bluebikes
        with st.spinner("Fetching nearest Bluebikes station…"):
            params = [bigquery.ScalarQueryParameter("mbta_station_id", "STRING", selected_station_id)]
            nearest_df = run_query(SQL_NEAREST_AVAIL, params, cache_key=f"nearest::{selected_station_id}")

        if nearest_df.empty:
            st.info("No nearby Bluebikes station found.")
            st.stop()

        row = nearest_df.iloc[0]

        # --- Display Info ---
        st.markdown(f"### 🚉 MBTA Station: **{row['mbta_station_name']}**")
        st.markdown(f"### 🚲 Nearest Bluebikes Station: **{row['bb_name']}**")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Distance (m)", f"{row['distance_m']:.0f}")
        c2.metric("Status Time (UTC)", str(row.get("status_snapshot_ts", ""))[:19])
        c3.metric("Is Renting", "✅" if str(row.get("is_renting")) in ("1","True","true") else "❌")
        c4.metric("Is Returning", "✅" if str(row.get("is_returning")) in ("1","True","true") else "❌")

        c5, c6 = st.columns(2)
        c5.metric("Bikes Available", int(row.get("num_bikes_available") or 0))
        c6.metric("Docks Available", int(row.get("num_docks_available") or 0))

        # --- Map ---
        mdf = pd.DataFrame([
            {"name": f"MBTA — {row['mbta_station_name']}", "lat": row["mbta_lat"], "lon": row["mbta_lng"], "color": [0, 102, 204, 220]},
            {"name": f"Bluebikes — {row['bb_name']}", "lat": row["bb_lat"], "lon": row["bb_lng"], "color": [0, 200, 100, 220]},
        ])
        line_df = pd.DataFrame([{
            "from_lon": row["mbta_lng"], "from_lat": row["mbta_lat"],
            "to_lon": row["bb_lng"], "to_lat": row["bb_lat"]
        }])

        view = pdk.ViewState(latitude=mdf["lat"].mean(), longitude=mdf["lon"].mean(), zoom=12)

        layer_points = pdk.Layer(
            "ScatterplotLayer", data=mdf,
            get_position='[lon, lat]', get_fill_color='color',
            get_radius=70, pickable=True
        )
        layer_line = pdk.Layer(
            "LineLayer", data=line_df,
            get_source_position='[from_lon, from_lat]',
            get_target_position='[to_lon, to_lat]',
            get_width=4, get_color=[255, 140, 0]
        )

        st.pydeck_chart(pdk.Deck(
            initial_view_state=view,
            layers=[layer_points, layer_line],
            tooltip={"text": "{name}"}
        ))
