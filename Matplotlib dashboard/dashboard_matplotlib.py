# Databricks notebook source
df = spark.table("techmtraining.default.gold_monthly_metrics")

# COMMAND ----------

df_q = spark.table("techmtraining.default.gold_quarterly_metrics")

# COMMAND ----------

gold_monthly = spark.table("techmtraining.default.gold_monthly_metrics")
gold_quarterly = spark.table("techmtraining.default.gold_quarterly_metrics")

# COMMAND ----------

spark.table("techmtraining.default.gold_airline_metrics").display()
spark.table("techmtraining.default.gold_airport_metrics").display()

# COMMAND ----------

from pyspark.sql.functions import count, avg

kpi_df = df.agg(
    count("*").alias("total_flights"),
    avg("avg_delay").alias("avg_delay")
)

display(kpi_df)

# COMMAND ----------

from pyspark.sql.functions import count, avg

monthly_df = (
    df.groupBy("year", "month")
      .agg(
          count("*").alias("flight_count"),
          avg("avg_delay").alias("avg_delay")
      )
      .orderBy("year", "month")
)

monthly_pd = monthly_df.toPandas()

# COMMAND ----------

from pyspark.sql.functions import avg

airline_df = (
    df.groupBy("year", "month")
      .agg(
          avg("avg_delay").alias("avg_delay")
      )
      .orderBy(col("avg_delay").desc())
)

display(airline_df)

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(16,10))

# KPI TEXT
plt.subplot(2,2,1)
plt.axis("off")
plt.text(0.1, 0.8, f"Total Flights: {int(kpi.total_flights)}", fontsize=14)
plt.text(0.1, 0.6, f"Delayed Flights: {int(kpi.delayed_flights)}", fontsize=14)
plt.text(0.1, 0.4, f"Cancelled Flights: {int(kpi.cancelled_flights)}", fontsize=14)
plt.text(0.1, 0.2, f"Avg Delay (min): {round(kpi.avg_delay,2)}", fontsize=14)

# Monthly Flight Volume
plt.subplot(2,2,2)
plt.bar(
    monthly_pd["month"].astype(str),
    monthly_pd["flight_count"]
)
plt.title("Monthly Flight Volume")
plt.xlabel("Month")
plt.ylabel("Flights")

# Monthly Avg Delay
plt.subplot(2,2,3)
plt.plot(
    monthly_pd["month"].astype(str),
    monthly_pd["avg_delay"],
    marker="o"
)
plt.title("Monthly Average Delay")
plt.xlabel("Month")
plt.ylabel("Delay (min)")

# Airline Delay Comparison
plt.subplot(2,2,4)
plt.barh(
    airline_pd["AIRLINE"],
    airline_pd["avg_delay"]
)
plt.title("Average Delay by Airline")
plt.xlabel("Delay (min)")

plt.suptitle("✈ Aviation Performance Dashboard", fontsize=16)
plt.tight_layout()
plt.show()