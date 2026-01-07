# Databricks notebook source
from pyspark.sql.functions import count, avg, when, col

# Aggregate Silver data into daily KPIs
# Gold tables contain only business metrics (no raw columns)

gold_flight_metrics = (
    spark.table("silver_flights_latest")
    .groupBy("flight_date")
    .agg(
        count("*").alias("total_flights"),
        count(when(col("is_delayed") == 1, True)).alias("delayed_flights"),
        count(when(col("cancelled") == 1, True)).alias("cancelled_flights"),
        avg("delay_minutes").alias("avg_delay_minutes")
    )
)

# Save Gold daily metrics into gold folder
gold_flight_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/FileStore/tables/aviation_project_lakshmi/gold/flight_metrics")

# COMMAND ----------

from pyspark.sql.functions import year, month

# Monthly aggregation for trend analysis

gold_monthly_metrics = (
    spark.table("silver_flights_latest")
    .withColumn("year", year("flight_date"))
    .withColumn("month", month("flight_date"))
    .groupBy("year", "month")
    .agg(
        count("*").alias("total_flights"),
        count(when(col("is_delayed") == 1, True)).alias("delayed_flights"),
        count(when(col("cancelled") == 1, True)).alias("cancelled_flights"),
        avg("delay_minutes").alias("avg_delay_minutes")
    )
)

gold_monthly_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true").save("dbfs:/FileStore/tables/aviation_project_lakshmi/gold/monthly_metrics")

# COMMAND ----------

from pyspark.sql.functions import quarter

# Quarterly aggregation for executive-level reporting

gold_quarterly_metrics = (
    spark.table("silver_flights_latest")
    .withColumn("year", year("flight_date"))
    .withColumn("quarter", quarter("flight_date"))
    .groupBy("year", "quarter")
    .agg(
        count("*").alias("total_flights"),
        count(when(col("is_delayed") == 1, True)).alias("delayed_flights"),
        avg("delay_minutes").alias("avg_delay_minutes")
    )
)

gold_quarterly_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true").save("dbfs:/FileStore/tables/aviation_project_lakshmi/gold/quarterly_metrics")