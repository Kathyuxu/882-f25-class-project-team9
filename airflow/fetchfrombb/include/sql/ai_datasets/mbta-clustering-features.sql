-- Feature engineering for MBTA station clustering
-- Identifies last-mile connectivity gaps by analyzing Bluebikes availability near MBTA stations
CREATE OR REPLACE TABLE `ba882-f25-class-project-team9.bluebikes_analysis.mbta_clustering_features`
AS

WITH mbta_stations AS (
    -- Deduplicated MBTA stations
    SELECT DISTINCT 
        station_id,
        station_name,
        lat,
        lng
    FROM `ba882-f25-class-project-team9.mbta_data.stations_dedup`
    WHERE lat IS NOT NULL 
      AND lng IS NOT NULL
),

bluebikes_stations AS (
    -- Active Bluebikes stations with coordinates
    SELECT DISTINCT
        station_id,
        lat,
        lon,
        station_type
    FROM `ba882-f25-class-project-team9.bluebikes_analysis.station_info`
    WHERE lat IS NOT NULL 
      AND lon IS NOT NULL
),

-- Feature 1: Distance to nearest Bluebikes station
nearest_bluebikes AS (
    SELECT 
        m.station_id as mbta_station_id,
        MIN(ST_DISTANCE(
            ST_GEOGPOINT(m.lng, m.lat),
            ST_GEOGPOINT(b.lon, b.lat)
        )) as distance_to_nearest_bluebikes_m,
        COUNT(CASE 
            WHEN ST_DISTANCE(
                ST_GEOGPOINT(m.lng, m.lat),
                ST_GEOGPOINT(b.lon, b.lat)
            ) <= 500 THEN 1 
        END) as bluebikes_within_500m
    FROM mbta_stations m
    CROSS JOIN bluebikes_stations b
    GROUP BY m.station_id
),

-- Feature 2: Morning rush availability (7-9am)
-- JOIN station_status with station_info to get coordinates
morning_availability AS (
    SELECT 
        m.station_id as mbta_station_id,
        COUNT(DISTINCT s.station_id) as bluebikes_nearby_count_morning,
        AVG(s.num_bikes_available) as avg_bikes_available_morning,
        AVG(s.num_docks_available) as avg_docks_available_morning
    FROM mbta_stations m
    CROSS JOIN `ba882-f25-class-project-team9.bluebikes_analysis.station_status` s
    INNER JOIN `ba882-f25-class-project-team9.bluebikes_analysis.station_info` si
        ON s.station_id = si.station_id
    WHERE EXTRACT(HOUR FROM TIMESTAMP_SECONDS(s.last_reported)) BETWEEN 7 AND 9
      AND ST_DISTANCE(
          ST_GEOGPOINT(m.lng, m.lat),
          ST_GEOGPOINT(si.lon, si.lat)  -- Use station_info for coordinates
      ) <= 800  -- Within 800m walking distance
    GROUP BY m.station_id
),

-- Feature 3: Evening rush availability (5-7pm)
evening_availability AS (
    SELECT 
        m.station_id as mbta_station_id,
        COUNT(DISTINCT s.station_id) as bluebikes_nearby_count_evening,
        AVG(s.num_bikes_available) as avg_bikes_available_evening,
        AVG(s.num_docks_available) as avg_docks_available_evening
    FROM mbta_stations m
    CROSS JOIN `ba882-f25-class-project-team9.bluebikes_analysis.station_status` s
    INNER JOIN `ba882-f25-class-project-team9.bluebikes_analysis.station_info` si
        ON s.station_id = si.station_id
    WHERE EXTRACT(HOUR FROM TIMESTAMP_SECONDS(s.last_reported)) BETWEEN 17 AND 19
      AND ST_DISTANCE(
          ST_GEOGPOINT(m.lng, m.lat),
          ST_GEOGPOINT(si.lon, si.lat)  -- Use station_info for coordinates
      ) <= 800
    GROUP BY m.station_id
),

-- Feature 4: Trip volume from MBTA stations to/from nearby Bluebikes
mbta_bluebikes_trips AS (
    SELECT
        m.station_id as mbta_station_id,
        COUNT(*) as total_bluebikes_trips_nearby,
        COUNT(CASE WHEN h.member_casual = 'member' THEN 1 END) as commuter_trips_nearby,
        AVG(TIMESTAMP_DIFF(h.ended_at, h.started_at, MINUTE)) as avg_trip_duration_min
    FROM mbta_stations m
    INNER JOIN `ba882-f25-class-project-team9.bluebikes_historical_us.JantoSep_historical` h
        ON ST_DISTANCE(
            ST_GEOGPOINT(m.lng, m.lat),
            ST_GEOGPOINT(h.start_lng, h.start_lat)
        ) <= 800
    GROUP BY m.station_id
),

-- Feature 5: MBTA ridership from historical data
mbta_ridership AS (
    SELECT
        start_station_id as station_id,
        COUNT(*) as mbta_trips_from_station,
        COUNT(DISTINCT route_id) as route_diversity
    FROM `ba882-f25-class-project-team9.mbta_data.mbta_historical`
    GROUP BY start_station_id
)

-- Final feature table
SELECT 
    m.station_id,
    m.station_name,
    m.lat,
    m.lng,
    
    -- Distance features
    COALESCE(nb.distance_to_nearest_bluebikes_m, 9999) as distance_to_nearest_bluebikes_m,
    COALESCE(nb.bluebikes_within_500m, 0) as bluebikes_within_500m,
    
    -- Morning rush features
    COALESCE(ma.bluebikes_nearby_count_morning, 0) as bluebikes_nearby_morning,
    COALESCE(ma.avg_bikes_available_morning, 0) as avg_bikes_available_morning,
    COALESCE(ma.avg_docks_available_morning, 0) as avg_docks_available_morning,
    
    -- Evening rush features
    COALESCE(ea.bluebikes_nearby_count_evening, 0) as bluebikes_nearby_evening,
    COALESCE(ea.avg_bikes_available_evening, 0) as avg_bikes_available_evening,
    COALESCE(ea.avg_docks_available_evening, 0) as avg_docks_available_evening,
    
    -- Trip volume features
    COALESCE(mbt.total_bluebikes_trips_nearby, 0) as bluebikes_trip_volume,
    COALESCE(mbt.commuter_trips_nearby, 0) as commuter_trip_volume,
    COALESCE(mbt.avg_trip_duration_min, 0) as avg_trip_duration_min,
    
    -- MBTA features
    COALESCE(mr.mbta_trips_from_station, 0) as mbta_ridership,
    COALESCE(mr.route_diversity, 0) as mbta_route_diversity,
    
    -- Derived features
    CASE 
        WHEN COALESCE(nb.distance_to_nearest_bluebikes_m, 9999) > 800 THEN 1
        ELSE 0
    END as is_isolated,
    
    CASE
        WHEN COALESCE(ma.avg_bikes_available_morning, 0) < 2 THEN 1
        ELSE 0
    END as has_low_morning_availability,
    
    'train' as split
    
FROM mbta_stations m
LEFT JOIN nearest_bluebikes nb ON m.station_id = nb.mbta_station_id
LEFT JOIN morning_availability ma ON m.station_id = ma.mbta_station_id
LEFT JOIN evening_availability ea ON m.station_id = ea.mbta_station_id
LEFT JOIN mbta_bluebikes_trips mbt ON m.station_id = mbt.mbta_station_id
LEFT JOIN mbta_ridership mr ON m.station_id = mr.station_id;