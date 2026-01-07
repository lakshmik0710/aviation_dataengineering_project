# Databricks notebook source
from pyspark.sql.functions import count, avg

# COMMAND ----------

from pyspark.sql.functions import col, count, when, avg
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

gold_flight_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/FileStore/tables/aviation_project_lakshmi/gold/flight_metrics")

# COMMAND ----------

from pyspark.sql.functions import year, month
spark.table("silver_flights_latest") \
.withColumn("year", year("flight_date")) \
.withColumn("month", month("flight_date")) \
.groupBy("year","month") \
.agg(
    count("*").alias("total_flights"),
    count(when(col("is_delayed")==1, True)).alias("delayed_flights"),
    avg("delay_minutes").alias("avg_delay_minutes")
) \
.write.format("delta").mode("overwrite") \
.save("dbfs:/FileStore/tables/aviation_project_lakshmi/gold/monthly_metrics");

# COMMAND ----------

from pyspark.sql.functions import quarter
spark.table("silver_flights_latest") \
.withColumn("year", year("flight_date")) \
.withColumn("quarter", quarter("flight_date")) \
.groupBy("year","quarter") \
.agg(
    count("*").alias("total_flights"),
    avg("delay_minutes").alias("avg_delay_minutes")
) \
.write.format("delta").mode("overwrite") \
.save("dbfs:/FileStore/tables/aviation_project_lakshmi/gold/quarterly_metrics");

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG techmtraining;
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold_flight_metrics
# MAGIC AS SELECT * FROM delta.`dbfs:/FileStore/tables/aviation_project_lakshmi/gold/flight_metrics`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold_monthly_metrics
# MAGIC AS SELECT * FROM delta.`dbfs:/FileStore/tables/aviation_project_lakshmi/gold/monthly_metrics`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold_quarterly_metrics
# MAGIC AS SELECT * FROM delta.`dbfs:/FileStore/tables/aviation_project_lakshmi/gold/quarterly_metrics`;

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/aviation_project_lakshmi/")

# COMMAND ----------

from pyspark.sql.functions import avg, sum, count

silver = spark.table("techmtraining.default.silver_flights_latest")

airline_gold = silver.groupBy("AIRLINE").agg(
    count("*").alias("total_flights"),
    avg("ARRIVAL_DELAY").alias("avg_delay"),
    sum("CANCELLED").alias("cancelled_flights")
)

airline_gold.write.mode("overwrite").saveAsTable(
    "techmtraining.default.gold_airline_metrics"
)

# COMMAND ----------

airport_gold = silver.groupBy("ORIGIN_AIRPORT").agg(
    count("*").alias("total_departures"),
    avg("ARRIVAL_DELAY").alias("avg_delay")
)

airport_gold.write.mode("overwrite").saveAsTable(
    "techmtraining.default.gold_airport_metrics"
)