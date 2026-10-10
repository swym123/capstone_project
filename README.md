# Banking ELT Data Pipeline

## 📌 Overview

An end-to-end **incremental ELT pipeline** for banking transaction data using **Apache Airflow, Snowflake, dbt, Python, SQL, Docker, and Git**.

The pipeline ingests CSV files into Snowflake and transforms them through multiple data warehouse layers.

## 🔄 Pipeline Architecture

```text
CSV Files
   ↓
Apache Airflow
   ↓
Snowflake RAW
   ↓
dbt STAGING
   ↓
dbt CORE
(Dimensions & Fact)
   ↓
dbt MART
(KPIs & Analytics)
```

## 🛠️ Tech Stack

* **Python** – Data ingestion
* **Apache Airflow** – Pipeline orchestration
* **Snowflake** – Data warehouse
* **dbt** – Data transformation & testing
* **SQL** – Data modeling & analytics
* **Docker** – Containerization
* **Git** – Version control

## ⚡ Key Features

* Incremental data processing
* Automated CSV ingestion
* RAW → STAGING → CORE → MART architecture
* Dimensional and fact modeling
* dbt data quality tests
* Analytics-ready KPI views
* Airflow end-to-end orchestration
* Docker-based environment

## 📊 Analytics

The MART layer provides metrics such as:

* Monthly transaction performance
* Customer performance
* Branch performance
* Payment method usage
* Loan exposure
* Complaint resolution
* Transaction success rates

## 🚀 Run

Start the Airflow environment:

```bash
cd airflow
docker compose up -d
```

Then open:

```text
http://localhost:8080
```

Login credentials:
- **Username:** `admin`
- **Password:** `admin`

Trigger the `banking_pipeline` DAG to run the complete pipeline.

## 👩‍💻 Author

**Tamanna Malviya**
