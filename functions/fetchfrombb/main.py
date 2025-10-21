import base64
import json
import requests
from datetime import datetime, timezone
from google.cloud import storage
import functions_framework
import os

BUCKET = os.getenv("BUCKET", "bluebikes-raw--f25-class-project-team9")
BASE_PREFIX = os.getenv("BASE_PREFIX", "gbfs/bluebikes")

FEEDS = {
    "station_information": "https://gbfs.lyft.com/gbfs/1.1/bos/en/station_information.json",
    "station_status": "https://gbfs.lyft.com/gbfs/1.1/bos/en/station_status.json",
    "system_regions": "https://gbfs.lyft.com/gbfs/1.1/bos/en/system_regions.json"
}

storage_client = storage.Client()


def upload_to_gcs(bucket_name, blob_name, text_data):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(text_data, content_type="application/x-ndjson")
    print(f" Uploaded {blob_name} ({len(text_data.splitlines())} lines)")


@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    try:
        message = cloud_event.data.get("message", {})
        data = message.get("data")
        if data:
            decoded = base64.b64decode(data).decode("utf-8")
            print(f" Received Pub/Sub message: {decoded}")
        else:
            print(" Received empty message (no data)")

        snapshot_ts = datetime.now(timezone.utc).isoformat()
        d = datetime.utcnow().strftime("%Y-%m-%d")
        hh = datetime.utcnow().strftime("%H")
        mm = datetime.utcnow().strftime("%M")

        results = {}

        for feed_name, url in FEEDS.items():
            try:
                print(f" Fetching {feed_name} from {url} ...")
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                payload = r.json()

                data_key = next(iter(payload.get("data", {}).keys()), None)
                rows = payload.get("data", {}).get(data_key, [])
                for row in rows:
                    row["snapshot_ts"] = snapshot_ts

                ndjson = "\n".join(json.dumps(r) for r in rows)
                blob_name = f"{BASE_PREFIX}/{feed_name}/dt={d}/HH={hh}/mm={mm}/part-000.ndjson"
                upload_to_gcs(BUCKET, blob_name, ndjson)
                results[feed_name] = len(rows)

            except Exception as e:
                print(f" Error fetching {feed_name}: {e}")
                results[feed_name] = str(e)

        print(" Snapshot complete:", json.dumps(results, ensure_ascii=False))
        return "OK", 200

    except Exception as e:
        print(f" Exception: {e}")
        return f"ERROR: {e}", 500


