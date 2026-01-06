# ✈️ Optimizing Aviation Insights  
## A Data Engineering Approach for Analyzing Flight Delays and Cancellations

---

## 📘 Project Overview

This project analyzes the on-time performance of domestic U.S. flights using a modern, end-to-end **data engineering pipeline built on Databricks**. The solution leverages **PySpark, Delta Lake, DBFS storage, and Matplotlib dashboards** to process historical and latest flight data using a **Bronze → Silver → Gold Medallion Architecture**.

The pipeline enables reliable KPI reporting, trend analysis, and business insights related to flight delays, cancellations, and operational performance.

---

## 🧱 Architecture Overview
---

## 🛠️ Tech Stack

- **Platform:** Databricks
- **Storage:** DBFS (FileStore)
- **Processing:** PySpark
- **Storage Format:** Delta Lake
- **Visualization:** Matplotlib
- **Version Control:** GitHub

---

## 📂 Data Sources

The following datasets are used (stored in DBFS):

| File Name | Description |
|----------|-------------|
| `airlines.csv` | Airline master data |
| `airports.csv` | Airport reference data |
| `flights_sample.csv` | Historical flight data |
| `flights_latest.csv` | Latest / near-real-time flight data |

---

## ⭐ Bronze Layer – Raw Ingestion

**Purpose:**
- Store raw data exactly as received
- Enable auditing and reprocessing

**Bronze Tables:**
- `bronze_airlines`
- `bronze_airports`
- `bronze_flights_sample`
- `bronze_flights_latest`

✔ No transformations  
✔ Schema matches source CSV files  
✔ Stored as Delta tables  

---

## ⭐ Silver Layer – Data Cleaning & Standardization

**Purpose:**
- Clean and standardize raw data
- Prepare analytics-ready datasets

**Silver Tables:**
- `silver_airlines`
- `silver_airports`
- `silver_flights_sample`
- `silver_flights_latest`

**Key Transformations:**
- Schema enforcement (STRING → INT / DOUBLE / DATE)
- Date parsing and standardization
- Null handling and invalid record removal
- Standardization of airline & airport codes
- Derived columns:
  - `TOTAL_DELAY`
  - `ON_TIME_FLAG`
  - `DELAY_CATEGORY`
  - `CANCELLED_FLAG`

✔ No aggregations  
✔ Granular, trusted data  

---

## ⭐ Gold Layer – Business Metrics

**Purpose:**
- Provide KPI-ready, aggregated datasets
- Power dashboards and analytics

**Gold Tables:**
- `gold_flight_metrics` – Daily KPIs
- `gold_monthly_metrics` – Monthly trends
- `gold_quarterly_metrics` – Quarterly performance

**Metrics Calculated:**
- Total flights
- Delayed flights
- Cancelled flights
- Average arrival delay
- Monthly & quarterly trends
- Airline performance insights

---

## 📊 Dashboard (Matplotlib)

Dashboards are built using **Python Matplotlib inside Databricks notebooks**, reading exclusively from **Gold tables**.

**Dashboard Visuals:**
- KPI Summary (Total Flights, Delays, Cancellations, Avg Delay)
- Monthly Flight Volume
- Delay Trends Over Time
- Airline-wise Delay Comparison
- Quarterly Performance Trends

✔ No SQL Warehouse  
✔ No external BI tools  
✔ Gold-layer driven analytics  

---

## 📁 Repository Structure
---

## 🎯 Project Outcome

- End-to-end data pipeline implemented on Databricks
- Clean Bronze → Silver → Gold data flow
- Business-ready KPIs and analytics
- Professional dashboards using Matplotlib
- Reproducible and interview-ready data engineering project

---

## 👩‍💻 Author

**Lakshmi**
