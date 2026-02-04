# Weather Data Pipeline (Ukraine)

## 📌 Project Overview

This project is an **end-to-end data engineering pipeline** that collects weather data for Ukrainian cities, stores raw data in MongoDB, transforms it, and loads curated data into PostgreSQL. The pipeline is orchestrated with **Apache Airflow** and runs on an hourly schedule.

The project is designed as a **pet project for a strong junior data engineer**, demonstrating core data engineering concepts: layering, idempotency, data quality checks, and orchestration.

---

## 🏗 Architecture

```
OpenWeather API
        |
        v
┌──────────────────┐
│  Airflow DAG     │  (hourly)
│  weather_pipeline│
└──────────────────┘
        |
        v
┌──────────────────┐        ┌────────────────────────────┐
│ MongoDB (RAW)    │ -----> │ PostgreSQL (STAGING)        │
│ weather_raw      │        │ staging.stg_weather_obs    │
└──────────────────┘        └────────────────────────────┘
```

### Data Layers

* **RAW (MongoDB)**
  Stores unmodified API responses with ingestion metadata.

* **STAGING (PostgreSQL)**
  Stores cleaned and validated weather observations with strict schema and constraints.

---

## 🧰 Tech Stack

* **Python 3.12**
* **Apache Airflow** (orchestration)
* **MongoDB** (raw data storage)
* **PostgreSQL** (staging / analytical storage)
* **psycopg2**, **pymongo**, **requests**
* **WSL (Ubuntu 24.04)** for local development

---

## 📂 Project Structure

```
weather_pipeline/
├── dags/
│   └── weather_pipeline/
│       ├── weather_pipeline_dag.py
│       ├── jobs/
│       │   ├── extract_weather.py
│       │   └── load_staging_weather.py
│       ├── config/
│       │   ├── settings.py
│       │   └── cities.py
│       └── db/
│           ├── mongo/
│           │   └── create_indexes.py
│           └── postgres/
│               ├── 01_create_user.sql
│               ├── 02_create_database.sql
│               ├── 03_create_schema.sql
│               ├── 04_create_tables.sql
│               └── 05_create_indexes.sql
├── .env.example
├── requirements.txt
└── README.md
```

---

## 🔄 Data Flow

1. **Extract**

   * Airflow task calls OpenWeather API
   * Raw response is saved to MongoDB
   * Unique index ensures idempotency

2. **Transform**

   * Raw MongoDB documents are validated and normalized
   * Invalid values are filtered or set to NULL

3. **Load**

   * Clean data is inserted into PostgreSQL staging table
   * `ON CONFLICT DO NOTHING` guarantees safe re-runs

---

## 🧪 Data Quality & Idempotency

* MongoDB unique index on:

  ```
  (city_id, observation_ts, source)
  ```

* PostgreSQL constraints:

  * CHECK constraints for ranges
  * NOT NULL on business-critical fields
  * UNIQUE constraint on natural key

These mechanisms allow:

* DAG retries
* Backfills
* Safe manual re-runs

---

## ⏱ Scheduling

* **Schedule**: `@hourly`
* **Catchup**: disabled
* **Retries**: enabled (configurable in DAG)

---

## 🔐 Configuration

All secrets and connection parameters are provided via environment variables.

Example `.env` file:

```env
OPENWEATHER_API_KEY=your_api_key

POSTGRESQL_USER=weather_user
POSTGRESQL_PASSWORD=secure_password
POSTGRESQL_HOST=localhost
POSTGRESQL_PORT=5432
POSTGRESQL_DB=weather

MONGO_URI=mongodb://localhost:27017
```

> ⚠️ `.env` is not committed to git. Use `.env.example` as a template.

---

## ▶️ How to Run Locally

1. Start MongoDB and PostgreSQL in WSL
2. Activate Python virtual environment
3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```
4. run db/mongo/create_indexes.py to create Mongo collection and indexes
5. run db/postgres/ scripts to create postgres table 01_create_user.sql add -v password='your_password' and run it and 02_create_database.sql from sudo
6. Initialize Airflow:

   ```bash
   airflow standalone
   ```
7. Open Airflow UI and trigger `weather_pipeline_dag`

---

## 🚀 Future Improvements

* Support multiple cities (dimension table)
* Add monitoring and metrics
* Add alerting on failures
* Add data mart / analytical layer
* Containerize with Docker

---

## 👤 Author

**OlehKhyl** — aspiring Data Engineer
This project was built as a learning and portfolio project following data engineering best practices.
