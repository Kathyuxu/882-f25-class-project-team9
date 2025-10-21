import pandas as pd
import requests
import zipfile
from io import BytesIO
import gc
from google.cloud import bigquery

def fetch_mbta_historical(request):
    """
    Cloud Function: Fetch MBTA Historical Data (Jan–Sep 2025) into BigQuery.
    Memory-optimized and date-filtered.
    """

    PROJECT_ID = "ba882-f25-class-project-team9"
    DATASET = "mbta_data"
    TABLE = "mbta_historical"
    ARCHIVE_URL = "https://cdn.mbta.com/archive/archived_feeds.txt"

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    try:
        # --- Load MBTA archived feeds index ---
        feeds = pd.read_csv(
            ARCHIVE_URL,
            names=["feed_start_date", "feed_end_date", "feed_version", "archive_url", "archive_note"],
            header=0
        )

        # --- Filter feeds by publish date Jan–Sep 2025 ---
        feeds["feed_start_date"] = pd.to_datetime(feeds["feed_start_date"], format="%Y%m%d", errors="coerce")
        feeds = feeds[
            (feeds["feed_start_date"] >= "2025-01-01") &
            (feeds["feed_start_date"] <= "2025-09-30")
        ].dropna(subset=["archive_url"])

        print(f"📊 Found {len(feeds)} valid archives between Jan–Sep 2025")

        if feeds.empty:
            return ("✅ No valid archives found (0 rows uploaded)", 200)

        total_uploaded = 0

        for i, row in enumerate(feeds.itertuples(), start=1):
            url = str(row.archive_url).strip()
            print(f"\n📦 ({i}/{len(feeds)}) Downloading {url}")

            try:
                r = requests.get(url, timeout=90)
                if r.status_code != 200:
                    print(f"⚠️ Skipping {url}: HTTP {r.status_code}")
                    continue

                with zipfile.ZipFile(BytesIO(r.content)) as z:
                    names = z.namelist()
                    if not {"stops.txt", "trips.txt", "stop_times.txt"}.issubset(names):
                        print(f"⚠️ Missing files in {url}")
                        continue

                    # --- Read minimal columns to save memory ---
                    stops = pd.read_csv(z.open("stops.txt"), usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"])
                    trips = pd.read_csv(z.open("trips.txt"), usecols=["route_id", "service_id", "trip_id", "direction_id"])
                    stop_times = pd.read_csv(z.open("stop_times.txt"), usecols=["trip_id", "stop_id", "stop_sequence", "arrival_time"], low_memory=False)

                # --- Compute first/last stops per trip ---
                stop_times = stop_times.sort_values(["trip_id", "stop_sequence"], ascending=True)
                first = stop_times.groupby("trip_id", as_index=False).first().rename(columns={
                    "stop_id": "stop_id_start",
                    "arrival_time": "arrival_time_start"
                })
                last = stop_times.groupby("trip_id", as_index=False).last().rename(columns={
                    "stop_id": "stop_id_end",
                    "arrival_time": "arrival_time_end"
                })

                merged = pd.merge(trips, first, on="trip_id", how="left")
                merged = pd.merge(merged, last, on="trip_id", how="left", suffixes=("", "_last"))

                stops_sub = stops.rename(columns={
                    "stop_id": "stop_ref",
                    "stop_name": "stop_name_ref",
                    "stop_lat": "stop_lat_ref",
                    "stop_lon": "stop_lon_ref"
                })

                merged = merged.merge(stops_sub, left_on="stop_id_start", right_on="stop_ref", how="left")
                merged = merged.merge(
                    stops_sub,
                    left_on="stop_id_end",
                    right_on="stop_ref",
                    how="left",
                    suffixes=("_start", "_end")
                )

                df = pd.DataFrame({
                    "trip_id": merged["trip_id"],
                    "route_id": merged["route_id"],
                    "service_id": merged["service_id"],
                    "start_station_name": merged["stop_name_ref_start"],
                    "start_station_id": merged["stop_id_start"],
                    "end_station_name": merged["stop_name_ref_end"],
                    "end_station_id": merged["stop_id_end"],
                    "start_lat": merged["stop_lat_ref_start"],
                    "start_lng": merged["stop_lon_ref_start"],
                    "end_lat": merged["stop_lat_ref_end"],
                    "end_lng": merged["stop_lon_ref_end"],
                    "scheduled_start_time": pd.to_datetime(merged["arrival_time_start"], errors="coerce"),
                    "scheduled_end_time": pd.to_datetime(merged["arrival_time_end"], errors="coerce"),
                    "direction_id": merged["direction_id"],
                    "collected_at": pd.Timestamp.utcnow()
                }).dropna(subset=["trip_id", "route_id"], how="any")

                # --- Keep only trips scheduled Jan–Sep 2025 ---
                df = df[
                    (df["scheduled_start_time"] >= "2025-01-01") &
                    (df["scheduled_start_time"] < "2025-10-01")
                ]

                if df.empty:
                    print(f"⚠️ No usable rows (filtered out) in {url}")
                    continue

                # --- Upload to BigQuery ---
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
                job.result()

                total_uploaded += len(df)
                print(f"✅ Uploaded {len(df)} rows from {url}")

                # --- Memory cleanup ---
                del df, stops, trips, stop_times, merged, first, last, stops_sub
                gc.collect()

            except Exception as e:
                print(f"❌ Error processing {url}: {e}")

        return (f"✅ MBTA Historical data loaded successfully — {total_uploaded} rows total", 200)

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return (f"❌ Error occurred: {str(e)}", 500)
