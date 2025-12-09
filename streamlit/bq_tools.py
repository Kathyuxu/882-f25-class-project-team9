# bq_tools.py
from typing import Dict, Any
from google.cloud import bigquery

PROJECT_ID = "ba882-f25-class-project-team9"

STATS_TABLE = (
    "ba882-f25-class-project-team9."
    "bluebikes_analysis.station_dow_hour_stats"
)
LOOKUP_TABLE = (
    "ba882-f25-class-project-team9."
    "bluebikes_analysis.station_lookup"
)

bq_client = bigquery.Client(project=PROJECT_ID)


def get_station_availability_by_name(
    station_name: str,
    day_of_week: int,
    start_hour: int,
    end_hour: int | None = None,
) -> Dict[str, Any]:
    if end_hour is None:
        end_hour = start_hour

    sql = f"""
    WITH target_station AS (
      SELECT start_station_id
      FROM `{LOOKUP_TABLE}`
      WHERE LOWER(start_station_name) = LOWER(@station_name)
      ORDER BY start_station_id
      LIMIT 1
    )
    SELECT
      t.start_station_id AS start_station_id,
      @station_name       AS station_name,
      @day_of_week        AS dow,
      @start_hour         AS start_hour,
      @end_hour           AS end_hour,
      AVG(ts.avg_trips_per_day_hour) AS avg_trips,
      AVG(ts.trips_count)            AS avg_trips_count
    FROM `{STATS_TABLE}` ts
    JOIN target_station t
      ON ts.start_station_id = t.start_station_id
    WHERE ts.dow = @day_of_week
      AND ts.hour BETWEEN @start_hour AND @end_hour
    GROUP BY
      t.start_station_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("station_name", "STRING", station_name),
            bigquery.ScalarQueryParameter("day_of_week", "INT64", day_of_week),
            bigquery.ScalarQueryParameter("start_hour", "INT64", start_hour),
            bigquery.ScalarQueryParameter("end_hour", "INT64", end_hour),
        ]
    )

    rows = list(bq_client.query(sql, job_config=job_config).result())

    if not rows or rows[0]["avg_trips"] is None:
        return {
            "station_name": station_name,
            "day_of_week": day_of_week,
            "start_hour": start_hour,
            "end_hour": end_hour,
            "avg_trips": 0.0,
            "avg_trips_count": 0.0,
            "has_data": False,
        }

    row = rows[0]
    return {
        "station_id": row["start_station_id"],
        "station_name": station_name,
        "day_of_week": day_of_week,
        "start_hour": start_hour,
        "end_hour": end_hour,
        "avg_trips": float(row["avg_trips"]),
        "avg_trips_count": float(row["avg_trips_count"]),
        "has_data": True,
    }
