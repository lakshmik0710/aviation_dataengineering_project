# Databricks notebook source
# MAGIC %sql
# MAGIC -- Dimension table to track historical changes in flight status
# MAGIC CREATE TABLE IF NOT EXISTS dim_flights (
# MAGIC     dim_flight_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     flight_number STRING,
# MAGIC     origin STRING,
# MAGIC     dest STRING,
# MAGIC     flight_status STRING,
# MAGIC     effective_start_date DATE,
# MAGIC     effective_end_date DATE,
# MAGIC     is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD Type-2 merge to preserve history of flight status changes
# MAGIC MERGE INTO dim_flights tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         FLIGHT_NUMBER,
# MAGIC         ORIGIN_AIRPORT AS origin,
# MAGIC         DESTINATION_AIRPORT AS dest,
# MAGIC         CASE
# MAGIC             WHEN cancelled = 1 THEN 'CANCELLED'
# MAGIC             WHEN is_delayed = 1 THEN 'DELAYED'
# MAGIC             ELSE 'ON_TIME'
# MAGIC         END AS flight_status
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY FLIGHT_NUMBER
# MAGIC                 ORDER BY flight_date DESC
# MAGIC             ) AS rn
# MAGIC         FROM silver_flights_latest
# MAGIC     ) s
# MAGIC     WHERE rn = 1
# MAGIC ) src
# MAGIC ON tgt.flight_number = src.FLIGHT_NUMBER
# MAGIC AND tgt.is_current = true
# MAGIC
# MAGIC WHEN MATCHED AND tgt.flight_status <> src.flight_status
# MAGIC THEN UPDATE SET
# MAGIC     tgt.is_current = false,
# MAGIC     tgt.effective_end_date = current_date()
# MAGIC
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC     flight_number,
# MAGIC     origin,
# MAGIC     dest,
# MAGIC     flight_status,
# MAGIC     effective_start_date,
# MAGIC     effective_end_date,
# MAGIC     is_current
# MAGIC )
# MAGIC VALUES (
# MAGIC     src.FLIGHT_NUMBER,
# MAGIC     src.origin,
# MAGIC     src.dest,
# MAGIC     src.flight_status,
# MAGIC     current_date(),
# MAGIC     DATE '9999-12-31',
# MAGIC     true
# MAGIC );