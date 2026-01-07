# Databricks notebook source
# Read the bronze flights_latest table as a DataFrame

from pyspark.sql.functions import col, when, to_date, lpad, concat

silver_flights_latest = (
    spark.table("bronze_flights_latest")
    .withColumn(
        "flight_date",
        to_date(
            concat(
                col("YEAR"),
                lpad(col("MONTH"), 2, "0"),
                lpad(col("DAY"), 2, "0")
            ),
            "yyyyMMdd"
        )
    )
    .withColumn("arrival_delay", col("ARRIVAL_DELAY").cast("double"))
    .withColumn("is_delayed", when(col("ARRIVAL_DELAY") > 0, 1).otherwise(0))
    .withColumn("delay_minutes", when(col("ARRIVAL_DELAY") > 0, col("ARRIVAL_DELAY")).otherwise(0))
    .withColumn("cancelled", col("CANCELLED").cast("int"))
    .dropna(subset=["flight_date", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"])
)

silver_flights_latest.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/FileStore/tables/aviation_project_lakshmi/silver/flights_latest")

# COMMAND ----------

from pyspark.sql.functions import col, when, to_date, lpad, concat

# Read the bronze_flights_sample table and transform columns for the silver layer
silver_flights_sample = (
    spark.table("bronze_flights_sample")
    # Construct flight_date from YEAR, MONTH, and DAY columns
    .withColumn(
        "flight_date",
        to_date(
            concat(
                col("YEAR"),
                lpad(col("MONTH"), 2, "0"),
                lpad(col("DAY"), 2, "0")
            ),
            "yyyyMMdd"
        )
    )
    # Cast ARRIVAL_DELAY to double
    .withColumn("arrival_delay", col("ARRIVAL_DELAY").cast("double"))
    # Flag if the flight was delayed
    .withColumn("is_delayed", when(col("ARRIVAL_DELAY") > 0, 1).otherwise(0))
    # Store delay minutes only if delayed, else 0
    .withColumn("delay_minutes", when(col("ARRIVAL_DELAY") > 0, col("ARRIVAL_DELAY")).otherwise(0))
    # Cast CANCELLED to integer
    .withColumn("cancelled", col("CANCELLED").cast("int"))
    # Drop rows with missing key fields
    .dropna(subset=["flight_date", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"])
)

# Write the cleaned DataFrame to the silver layer as a Delta table
silver_flights_sample.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/FileStore/tables/aviation_project_lakshmi/silver/flights_sample")

# COMMAND ----------

# Write the bronze_airlines table to the silver layer as a Delta table
spark.table("bronze_airlines").write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/FileStore/tables/aviation_project_lakshmi/silver/airlines")

# Write the bronze_airports table to the silver layer as a Delta table
spark.table("bronze_airports").write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/FileStore/tables/aviation_project_lakshmi/silver/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG techmtraining;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_airlines
# MAGIC AS SELECT * FROM delta.`dbfs:/FileStore/tables/aviation_project_lakshmi/silver/airlines`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_airports
# MAGIC AS SELECT * FROM delta.`dbfs:/FileStore/tables/aviation_project_lakshmi/silver/airports`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_flights_sample
# MAGIC AS SELECT * FROM delta.`dbfs:/FileStore/tables/aviation_project_lakshmi/silver/flights_sample`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_flights_latest
# MAGIC AS SELECT * FROM delta.`dbfs:/FileStore/tables/aviation_project_lakshmi/silver/flights_latest`;

# COMMAND ----------

