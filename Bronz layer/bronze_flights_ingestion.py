# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog techmtraining;
# MAGIC use schema default;

# COMMAND ----------

# Read the airlines.csv file into a Spark DataFrame with header and schema inference
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("dbfs:/FileStore/tables/aviation_project_lakshmi/raw/airlines.csv")
)

# Write the DataFrame as a Delta table in overwrite mode to the specified path
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/FileStore/tables/aviation_project_lakshmi/bronze/airlines")

# COMMAND ----------

# Read the airports.csv file into a Spark DataFrame with header and schema inference
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("dbfs:/FileStore/tables/aviation_project_lakshmi/raw/airports.csv")
)

# Write the DataFrame as a Delta table in overwrite mode to the specified path
df.write.format("delta").mode("overwrite").save(
    "dbfs:/FileStore/tables/aviation_project_lakshmi/bronze/airports"
)

# COMMAND ----------

# Read the flights_sample.csv file into a Spark DataFrame with header and schema inference
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("dbfs:/FileStore/tables/aviation_project_lakshmi/raw/flights_sample.csv")
)

# Write the DataFrame as a Delta table in overwrite mode to the specified path
df.write.format("delta").mode("overwrite").save(
    "dbfs:/FileStore/tables/aviation_project_lakshmi/bronze/flights_sample"
)

# COMMAND ----------

# Read the flights_latest.csv file into a Spark DataFrame with header and schema inference
bronze_flights_latest = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("dbfs:/FileStore/tables/aviation_project_lakshmi/raw/flights_latest.csv")
)

# Write the DataFrame as a Delta table in overwrite mode to the specified path
bronze_flights_latest.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/FileStore/tables/aviation_project_lakshmi/bronze/flights_latest")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG techmtraining;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_airlines
# MAGIC AS SELECT * FROM delta.`dbfs:/FileStore/tables/aviation_project_lakshmi/bronze/airlines`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_airports
# MAGIC AS SELECT * FROM delta.`dbfs:/FileStore/tables/aviation_project_lakshmi/bronze/airports`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_flights_sample
# MAGIC AS SELECT * FROM delta.`dbfs:/FileStore/tables/aviation_project_lakshmi/bronze/flights_sample`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_flights_latest
# MAGIC AS SELECT * FROM delta.`dbfs:/FileStore/tables/aviation_project_lakshmi/bronze/flights_latest`;