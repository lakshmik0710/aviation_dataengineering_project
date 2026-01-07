# Databricks notebook source
# Read Silver table
silver_df = spark.table("techmtraining.default.silver_flights_latest")

# COMMAND ----------

from pyspark.sql.functions import count, avg

# Group the silver DataFrame by year and month, and calculate total flights and average delay
monthly_df = (
    silver_df
    .groupBy("year", "month")
    .agg(
        count("*").alias("total_flights"),
        avg("AIRLINE_DELAY").alias("avg_delay")
    )
)
# Overwrite the table and update its schema to match the DataFrame
monthly_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("techmtraining.default.gold_monthly_metrics")

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, to_date, year, quarter, count, avg

# Create a date column from YEAR, MONTH, DAY
silver_df_with_date = silver_df.withColumn(
    "FL_DATE",
    to_date(
        concat_ws("-", col("YEAR"), col("MONTH"), col("DAY")),
        "yyyy-M-d"
    )
)

# Group by year and quarter extracted from the new FL_DATE column
quarterly_df = (
    silver_df_with_date
    .groupBy(
        year("FL_DATE").alias("year"),
        quarter("FL_DATE").alias("quarter")
    )
    .agg(
        count("*").alias("total_flights"),
        avg("ARRIVAL_DELAY").alias("avg_delay")
    )
)

# Overwrite the table with the new quarterly metrics
quarterly_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    "techmtraining.default.gold_quarterly_metrics"
)