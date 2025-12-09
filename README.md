# 🚇🚲 MBTA & Bluebikes Integration Pipeline

[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Astronomer-017CEE?logo=apache-airflow)](https://www.astronomer.io/)
[![Google Cloud](https://img.shields.io/badge/Google%20Cloud-Platform-4285F4?logo=google-cloud)](https://cloud.google.com/)
[![License](https://img.shields.io/badge/license-Academic-green.svg)](LICENSE)

> **Real-time data pipeline analyzing "last-mile" transportation effectiveness in the Boston area**

## 👥 Team Members

- **Yu Xu** - [@Kathyuxu](https://github.com/Kathyuxu)
- **Nilay Jaini** - [@nilayjaini](https://github.com/nilayjaini)
- **Shuomeng Guan** - [@eveningeve](https://github.com/eveningeve)
- **YiFeng Chen** - [@esen113](https://github.com/esen113)

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Key Features](#-key-features)
- [Architecture](#-architecture)
- [Technology Stack](#-technology-stack)
- [Data Pipelines](#-data-pipelines)
- [Machine Learning Models](#-machine-learning-models)
- [Interactive Dashboard](#-interactive-dashboard)
- [Getting Started](#-getting-started)
- [Project Structure](#-project-structure)
- [Data Quality & Validation](#-data-quality--validation)
- [Key Findings](#-key-findings)
- [Future Improvements](#-future-improvements)
- [Resources](#-resources)
- [License](#-license)

---

## 🎯 Project Overview

This project builds a **real-time data pipeline** integrating MBTA (Massachusetts Bay Transportation Authority) transit data with Bluebikes bike-share data to analyze "last-mile" transportation effectiveness in the Boston metropolitan area.

### 🔍 Research Question
**"Is the last mile being served effectively?"**

We evaluate how well Bluebikes complements MBTA service by examining:
- 📍 Station proximity and accessibility
- 🚴 Bike availability patterns during peak transit hours
- 🔄 Usage coordination between transit and bike-share systems
- 🗺️ Geographic coverage and transit desert identification

---

## ✨ Key Features

### Real-Time Data Integration
- ⏱️ **MBTA data** refreshed every 2 minutes
- 🔄 **Bluebikes data** updated every 5 minutes
- 📊 Historical data retention for trend analysis

### Advanced Analytics
- 🤖 **ML-powered occupancy prediction** for MBTA vehicles
- 📈 **Demand forecasting** for Bluebikes stations
- 🎯 **Transit desert identification** using K-means clustering
- 📐 **Spatial analysis** of last-mile connectivity

### Interactive Visualization
- 🗺️ Real-time station availability maps
- 📊 Performance dashboards with key metrics
- 🔮 Predictive analytics visualizations
- 💡 Actionable recommendations

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
├──────────────────────────────┬──────────────────────────────────┤
│     MBTA v3 API              │     Bluebikes GBFS API           │
│  (/routes, /predictions,     │  (station_info, station_status,  │
│   /vehicles)                 │   system_regions)                │
└──────────┬───────────────────┴──────────────┬───────────────────┘
           │                                  │
           ▼                                  ▼
   ┌───────────────┐                 ┌───────────────┐
   │   Astronomer  │                 │ Cloud Function│
   │    Airflow    │                 │   + Pub/Sub   │
   │  (Every 2min) │                 │  (Every 5min) │
   └───────┬───────┘                 └───────┬───────┘
           │                                  │
           ▼                                  ▼
   ┌─────────────────────────────────────────────────┐
   │         Google Cloud Storage (Raw Zone)          │
   │           Timestamped JSON Files                 │
   └─────────────────┬───────────────────────────────┘
                     │
                     ▼
   ┌─────────────────────────────────────────────────┐
   │        BigQuery Staging Tables                   │
   └─────────────────┬───────────────────────────────┘
                     │
                     ▼
   ┌─────────────────────────────────────────────────┐
   │   BigQuery Base Tables (Partitioned & Merged)   │
   │  • rt_trip_stop_predictions                     │
   │  • rt_vehicle_positions                         │
   │  • station_status                               │
   │  • station_info                                 │
   └─────────────────┬───────────────────────────────┘
                     │
           ┌─────────┴─────────┐
           ▼                   ▼
   ┌───────────────┐   ┌──────────────┐
   │  BigQuery ML  │   │  Streamlit   │
   │    Models     │   │  Dashboard   │
   └───────────────┘   └──────────────┘
```

### Pipeline Comparison

| Aspect | MBTA Pipeline | Bluebikes Pipeline |
|--------|--------------|-------------------|
| **Pattern** | EtLT (Extract, transform, Load, Transform) | ELT (Extract, Load, Transform) |
| **Orchestration** | Astronomer Airflow DAG | Cloud Functions + Pub/Sub |
| **Frequency** | Every 2 minutes | Every 5 minutes |
| **Storage** | GCS → BigQuery Staging → BigQuery Base | GCS → BigQuery Staging → BigQuery Base |
| **Transformation** | Light (Python) + Heavy (SQL) | Heavy (SQL only) |

---

## 🛠️ Technology Stack

| Layer | Technologies |
|-------|-------------|
| **Orchestration** | Apache Airflow (Astronomer), Google Cloud Functions, Pub/Sub |
| **Storage** | Google Cloud Storage (Raw Zone), BigQuery (Data Warehouse) |
| **Processing** | Python 3.9+, BigQuery SQL |
| **ML/Analytics** | BigQuery ML (Logistic Regression, Linear Regression, K-Means) |
| **Visualization** | Streamlit, Plotly, Folium |
| **APIs** | MBTA v3 API, Bluebikes GBFS API |
| **Version Control** | Git, GitHub |
| **Project Management** | Jira (Scrum) |

---

## 🔄 Data Pipelines

### 📍 MBTA Real-Time Pipeline

**Architecture Pattern:** EtLT (Extract, transform, Load, Transform)

#### Data Sources
- `/routes` - Route information (subway, bus, commuter rail)
- `/predictions` - Real-time arrival/departure predictions
- `/vehicles` - Vehicle GPS positions and occupancy status

#### Key Tables

**Fact Tables:**
- `rt_trip_stop_predictions` - Real-time prediction events with arrival/departure timestamps
- `rt_vehicle_positions` - Vehicle location snapshots with occupancy levels

**Dimension Views:**
- `dim_route_v` - Route details (type, name, color)
- `dim_stop_v` - Stop/station information with coordinates
- `dim_trip_v` - Trip metadata (headsign, direction)
- `dim_vehicle_v` - Vehicle characteristics

#### Orchestration
**Airflow DAG:** `mbta_realtime_to_bigquery`
- ⏱️ Runs every 2 minutes
- 📥 Extracts data from 3 API endpoints in parallel
- 💾 Loads raw JSON to GCS with timestamps
- 🔄 Stages data in BigQuery
- 🔀 Merges into partitioned base tables with deduplication

#### Data Quality Measures
- SHA1-based deduplication keys
- Timestamp normalization to UTC
- Partition pruning by `DATE(observed_at_utc)`
- Service date calculation: `observed_at_utc - 3 hours` (MBTA convention)
- Filters: `stop_id IS NOT NULL` for predictions

---

### 🚴 Bluebikes Real-Time Pipeline

**Architecture Pattern:** ELT (Extract, Load, Transform)

#### Data Source Evaluation
Initially assessed 6 GBFS API endpoints:
- `ebikes_at_stations` ❌ (no data)
- `system_information` ❌ (no data)
- `free_bike_status` ❌ (no data)
- `station_information` ✅
- `station_status` ✅
- `system_regions` ✅

**Selected:** Only 3 populated endpoints

#### Key Tables

**Fact Table:**
- `station_status` - Real-time bike and dock availability
  - `num_bikes_available`
  - `num_docks_available`
  - `is_installed`, `is_renting`, `is_returning`
  - Last reported timestamp

**Dimension Table:**
- `station_info` - Static station details with merged region data
  - Station name, address, coordinates
  - Capacity and rental methods
  - Region information (merged from `system_regions`)

#### Transformations
- Dropped irrelevant columns (e.g., `legacy_id`)
- Consolidated `rental_methods` array into comma-delimited string
- Merged `system_regions` into `station_information` for unified dimension

#### Automation
- **Trigger:** Pub/Sub messages every 5 minutes
- **Processor:** Cloud Function (`bluebikes_gbfs_to_gcs`)
- **Flow:** API → GCS → BigQuery Staging → BigQuery Base

---

## 🤖 Machine Learning Models

### 1. MBTA Occupancy Prediction

**Model Type:** Logistic Regression (BigQuery ML)

**Purpose:** Predict vehicle crowding levels to help passengers find less congested options

**Configuration:**
```sql
AUTO_CLASS_WEIGHTS = TRUE
MAX_ITERATIONS = 50
```

**Features (7 total):**
- `hour_of_day` (0-23)
- `day_of_week` (1-7)
- `is_weekend` (boolean)
- `lat_bin`, `lon_bin` (coarse geographic bins)
- `route_id` (categorical)
- `current_status` (STOPPED_AT, IN_TRANSIT_TO, INCOMING_AT)

**Training Strategy:**
- **Window:** Rolling 60-day training window (T-67d to T-7d)
- **Validation:** Last 7 days for evaluation
- **Schedule:** Weekly retrain (Mondays at 5:00 AM)
- **Orchestration:** Airflow DAG `occ_train_weekly`

**Results:**
- ✅ Fast convergence with class weight balancing
- 📊 Route and time-of-day are strongest predictors
- 🗺️ Coarse geo-bins improve generalization across the network

**Artifacts:**
- `mbta_ml.occ_features_min` - Feature engineering table
- `mbta_ml.occ_lr_min` - Trained model
- `mbta_ml.occ_eval_last` - Evaluation metrics

---

### 2. Transit Desert Identification

**Model Type:** K-Means Clustering

**Purpose:** Identify MBTA stations with poor Bluebikes connectivity

**Features (10 engineered):**
1. **Spatial Proximity**
   - Distance to nearest Bluebikes station (m)
   - Number of Bluebikes stations within 400m radius
   - Number of Bluebikes stations within 800m radius

2. **Temporal Availability**
   - Average bike availability during morning peak (7-9 AM)
   - Average bike availability during evening peak (5-7 PM)
   - Minimum bikes available during peak hours

3. **Ridership Patterns**
   - MBTA station usage volume (entries/exits)
   - Bluebikes demand at nearby stations
   - Correlation between MBTA arrivals and Bluebikes pickups

**Classification Logic:**
- **Transit Desert:** Distance to nearest Bluebikes > 800m OR < 2 bikes during morning peak
- **Well-Served:** Meets proximity and availability thresholds

**Current Limitations & Phase 3 Improvements:**
1. ⚠️ Insufficient deduplication (514 vs. 153 actual stations)
2. ⚠️ All stations currently classified as deserts (threshold miscalibration)
3. 🔄 **Planned fixes:**
   - Implement run versioning
   - Switch to percentile-based classification
   - Validate against ground-truth assessments

---

### 3. Bluebikes Demand Prediction

**Model Type:** Linear Regression (BigQuery ML), ARIMA, DNN, XGBoost, Random Forest

**Purpose:** Forecast future Bluebikes demand for proactive bike redistribution

**Features:**
- Historical pickup/drop-off patterns
- Time features (hour, day of week, month)
- Weather conditions (if available)
- Nearby events or holidays
- MBTA service disruptions

**Outputs:**
- Predicted pickups by station and time
- Predicted drop-offs by station and time
- Net demand (pickups - drop-offs)

**Use Cases:**
- 🚚 Optimize rebalancing truck routes
- 📍 Identify stations requiring bike replenishment
- 🔮 Anticipate capacity constraints

**Integration:**
- Automated training via Airflow DAG
- Predictions stored in BigQuery
- Real-time visualization in Streamlit dashboard

**Best Model:** ARIMA


---

## 📊 Interactive Dashboard

Built with **Streamlit**, the dashboard provides real-time insights and interactive visualizations.

### Dashboard Pages

#### 1. 📈 Review_MBTA
- Historical system performance metrics
- Ridership trends over time
- Route-level analysis
- Vehicle utilization patterns

#### 2. 🚴 Review_BB
- Bluebikes usage statistics
- Top start/end stations (ranked by trip volume)
- Geographic distribution of ridership
- Temporal patterns (hourly, daily, seasonal)

#### 3. 🗺️ Overview
- **Last-Mile Geo Analysis**
  - Interactive map showing MBTA stations
  - Color-coded by Bluebikes proximity
  - 400m and 800m radius circles
- Real-time station status
- Coverage heatmaps

#### 4. 📋 Summary
- **Key Statistics:**
  - 333 / 514 MBTA stations meet one-mile rule (64.8%)
  - 181 stations do not meet criteria (35.2%)
- Distance distribution histogram
- Coverage metrics by line/route
- Recommendations summary

#### 5. 💡 Recommendations
- **Short-term** (0-6 months)
  - Priority station list for new Bluebikes docks
  - Quick-win locations
- **Medium-term** (6-12 months)
  - Expansion plan for underserved areas
  - Pilot program suggestions
- **Long-term** (1-2 years)
  - Strategic network growth
  - Integration with other mobility services

#### 6. 🔮 Occupancy Prediction
- Real-time MBTA vehicle crowding forecasts
- Probability of high/low occupancy by:
  - Route
  - Time of day
  - Direction
- Alternative route suggestions
- Historical accuracy metrics

#### 7. 🎯 Bluebikes Clustering
- K-Means model visualization
- **Transit Desert Identification:**
  - Map view with cluster assignments
  - Station-level connectivity scores
  - Poor connectivity areas highlighted
- Feature importance analysis

#### 8. 📊 Bluebikes Demand Prediction
- Forecasted pickups and drop-offs by station
- **Top Predicted Demand Stations:**
  - Next hour
  - Next 4 hours
  - Next day
- Time series charts
- Rebalancing recommendations

#### 9. 🔍 Availability Search
- **Real-time Station Finder:**
  - Search any MBTA station by name
  - View nearest Bluebikes stations instantly
  - Display current bike and dock availability
- Walking distance calculations
- Station status indicators

#### 10. 🔍 AI Transit Assistant
- **Real-time Station Finder:**
  - Search any MBTA station by name
  - View nearest Bluebikes stations instantly
  - Display current bike and dock availability
- Walking distance calculations
- Station status indicators

#### 11. 🔍 Bluebikes & Policy Assistant
- **Real-time Station Finder:**
  - Search any MBTA station by name
  - View nearest Bluebikes stations instantly
  - Display current bike and dock availability
- Walking distance calculations
- Station status indicators
---

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- Google Cloud Platform account
- Astronomer account (for Airflow)
- MBTA API key ([Get here](https://www.mbta.com/developers))
- Access to Bluebikes GBFS API

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/Kathyuxu/882-f25-class-project-team9.git
cd 882-f25-class-project-team9
```

2. **Set up Python environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env with your API keys and GCP credentials
```

4. **Set up GCP resources**
```bash
# Create GCS buckets
gsutil mb gs://YOUR_PROJECT_mbta_raw
gsutil mb gs://YOUR_PROJECT_bluebikes_raw

# Create BigQuery datasets
bq mk --dataset YOUR_PROJECT:mbta_staging
bq mk --dataset YOUR_PROJECT:mbta_base
bq mk --dataset YOUR_PROJECT:bluebikes_staging
bq mk --dataset YOUR_PROJECT:bluebikes_base
bq mk --dataset YOUR_PROJECT:mbta_ml
```

5. **Deploy Airflow DAGs**
```bash
astro dev start  # For local development
# Or push to Astronomer Cloud
astro deploy
```

6. **Deploy Cloud Functions**
```bash
cd functions/bluebikes_gbfs_to_gcs
gcloud functions deploy bluebikes_gbfs_to_gcs \
  --runtime python39 \
  --trigger-topic bluebikes-trigger \
  --entry-point main
```

7. **Run Streamlit dashboard**
```bash
cd streamlit
streamlit run app.py
```

---

## 📁 Project Structure

```
882-f25-class-project-team9/
├── .devcontainer/           # VS Code dev container config
├── airflow/                 # Astronomer Airflow project
│   ├── dags/
│   │   ├── mbta_realtime_to_bigquery.py
│   │   ├── occ_train_weekly.py
│   │   └── bluebikes_ml_pipeline.py
│   ├── include/
│   │   ├── sql/              # BigQuery SQL templates
│   │   └── utils/            # Helper functions
│   └── requirements.txt
├── functions/               # Google Cloud Functions
│   ├── bluebikes_gbfs_to_gcs/
│   │   ├── main.py
│   │   └── requirements.txt
│   └── bluebikes_stations_status_from_gcs/
│       ├── main.py
│       └── requirements.txt
├── streamlit/               # Streamlit dashboard
│   ├── pages/
│   │   ├── 1_Review_MBTA.py
│   │   ├── 2_Review_BB.py
│   │   ├── 3_Overview.py
│   │   ├── 4_Summary.py
│   │   ├── 5_Recommendations.py
│   │   ├── 6_Occupancy_Prediction.py
│   │   ├── 7_Bluebikes_Clustering.py
│   │   ├── 8_Bluebikes_Demand_Prediction.py
│   │   └── 9_Availability.py
│   ├── app.py               # Main dashboard entry
│   ├── utils.py             # Shared utilities
│   └── requirements.txt
├── failure/                 # Failed experiment documentation
├── README.md
├── .gitignore
└── LICENSE
```

---

## ✅ Data Quality & Validation

### MBTA Pipeline
- ✅ **Deduplication:** SHA1-based composite keys prevent duplicate events
- ✅ **Timestamp Normalization:** All timestamps converted to UTC
- ✅ **Partition Pruning:** Efficient querying via `DATE(observed_at_utc)` partitioning
- ✅ **Data Filtering:** Predictions with `stop_id IS NULL` are excluded
- ✅ **Service Date Logic:** Follows MBTA convention (`observed_at_utc - 3 hours`)
- ✅ **Schema Validation:** Enforced data types and constraints

### Bluebikes Pipeline
- ✅ **Endpoint Validation:** Only populated API endpoints selected (3/6)
- ✅ **Column Cleanup:** Irrelevant fields removed to reduce storage costs
- ✅ **Data Normalization:** Arrays converted to comma-delimited strings
- ✅ **Dimension Enrichment:** Regions merged into station info for single source of truth
- ✅ **Freshness Monitoring:** Timestamp checks to detect stale data

---

## 🔍 Key Findings

### Last-Mile Coverage Analysis

**Overall Statistics:**
- ✅ **64.8%** of MBTA stations (333/514) meet the one-mile connectivity rule
- ⚠️ **35.2%** of stations (181/514) lack adequate Bluebikes access
- 📏 Median distance to nearest Bluebikes station: **420 meters**

**By Transit Line:**
| Line | Stations | Well-Served | Coverage |
|------|----------|-------------|----------|
| Red Line | 22 | 20 | 90.9% |
| Orange Line | 20 | 18 | 90.0% |
| Green Line | 52 | 44 | 84.6% |
| Blue Line | 12 | 9 | 75.0% |
| Commuter Rail | 138 | 76 | 55.1% |

**Transit Deserts Identified:**
- Outer suburbs along commuter rail lines
- Residential areas in Dorchester and Mattapan
- Industrial zones near Chelsea and Everett

### Demand Patterns

**Peak Usage Hours:**
- Morning: 7:00-9:00 AM (commute to work/school)
- Evening: 5:00-7:00 PM (commute home)
- Lunch: 12:00-1:00 PM (midday trips)

**Occupancy Insights:**
- Red Line southbound: High occupancy 8:00-9:00 AM
- Orange Line northbound: High occupancy 5:30-6:30 PM
- Green Line: Consistently moderate occupancy

---

## 🔮 Future Improvements

### Phase 3 Roadmap

#### Data Pipeline Enhancements
- [ ] Implement data quality monitoring dashboard
- [ ] Add anomaly detection for API failures
- [ ] Expand to include weather data for demand prediction
- [ ] Integrate real-time event data (concerts, sports games)

#### Machine Learning Improvements
- [ ] Fix transit desert clustering deduplication
- [ ] Switch to percentile-based classification
- [ ] Implement deep learning models (LSTM) for demand forecasting
- [ ] Add explainability features (SHAP values) for model predictions

#### Dashboard Features
- [ ] Mobile-responsive design
- [ ] User authentication and personalized recommendations
- [ ] Email alerts for service disruptions
- [ ] Integration with trip planning apps

#### Research Extensions
- [ ] Analyze equity implications of last-mile gaps
- [ ] Study impact of weather on mode choice
- [ ] Compare Boston with other cities (NYC, SF, DC)
- [ ] Evaluate electric scooter integration potential

---

## 📚 Resources

### Project Links
- **GitHub Repository:** [882-f25-class-project-team9](https://github.com/Kathyuxu/882-f25-class-project-team9)
- **Jira Board:** [Project Backlog](https://kathyxu.atlassian.net/jira/software/projects/SCRUM/boards/1/backlog)
- **Phase 2 Slides:** [Presentation](https://docs.google.com/presentation/d/1Xad4EMYtHJDbGBUnD0eceDAAJnp_ZuNxo4-JTpAIwfg/edit)
- **Streamlit Dashboard:** Coming soon!

### Data Sources
- **MBTA Developer Portal:** [https://www.mbta.com/developers](https://www.mbta.com/developers)
- **Bluebikes System Data:** [https://bluebikes.com/system-data](https://bluebikes.com/system-data)
- **Policy Website:**: [https://www.mbta.com/policies](https://www.mbta.com/policies)
                       [https://www.mbta.com/bikes](https://www.mbta.com/bikes)
                       [https://bluebikes.com/how-it-works]( https://bluebikes.com/how-it-works)
                       [https://bluebikes.com/pricing](https://bluebikes.com/pricing)
                       [https://bluebikes.com/system-data](https://bluebikes.com/system-data)

### Documentation
- [MBTA API Documentation](https://www.mbta.com/developers/v3-api)
- [GBFS Specification](https://github.com/MobilityData/gbfs)
- [BigQuery ML Documentation](https://cloud.google.com/bigquery/docs/bqml-introduction)
- [Astronomer Airflow Docs](https://docs.astronomer.io/)

---

## 📄 License

This project is an academic assignment for **BA882 - Fall 2025** at Questrom School of Business, Boston University.

**Academic Use Only** - Not for commercial redistribution.

---

## 🙏 Acknowledgments

- **Professor:** Dr. Brock Tibert, BA882 - Fall 2025
- **MBTA:** For providing comprehensive open data APIs
- **Bluebikes:** For maintaining GBFS-compliant real-time data feeds
- **Astronomer:** For Airflow hosting and support
- **Google Cloud Platform:** For generous education credits

---

## 📞 Contact

For questions, feedback, or collaboration opportunities:
- **Email:** Contact team members via GitHub profiles

---

<div align="center">

**Built with ❤️ in Boston**

*Improving urban mobility, one data point at a time*

</div>
