# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-8382e200-81c0-4bc3-9bdb-6aee604b0a8c
# MAGIC %md
# MAGIC # Revenue by Traffic Lab
# MAGIC Get the 3 traffic sources generating the highest total revenue.
# MAGIC 1. Aggregate revenue by traffic source
# MAGIC 2. Get top 3 traffic sources by total revenue
# MAGIC 3. Clean revenue columns to have two decimal places
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**, **`sort`**, **`limit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`alias`**, **`desc`**, **`cast`**, **`operators`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-in Functions</a>: **`avg`**, **`sum`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.06L

# COMMAND ----------

# DBTITLE 0,--i18n-b6ac5716-1668-4b34-8343-ee2d5c77cfad
# MAGIC %md
# MAGIC ### Setup
# MAGIC Run the cell below to create the starting DataFrame **`df`**.

# COMMAND ----------

from pyspark.sql.functions import col

# Purchase events logged on the BedBricks website
df = (spark.table("events")
      .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
      .filter(col("revenue").isNotNull())
      .drop("event_name")
     )

display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-78acde42-f2b7-4b0f-9760-65fba886ef5b
# MAGIC %md
# MAGIC
# MAGIC ### 1. Aggregate revenue by traffic source
# MAGIC - Group by **`traffic_source`**
# MAGIC - Get sum of **`revenue`** as **`total_rev`**. Round this to the tens decimal place (e.g. `nnnnn.n`). 
# MAGIC - Get average of **`revenue`** as **`avg_rev`**
# MAGIC
# MAGIC Remember to import any necessary built-in functions.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import avg, col, sum

traffic_df = (df
              .groupBy("traffic_source")
              .agg(sum(col("revenue")).alias("total_rev"),
                   avg(col("revenue")).alias("avg_rev"))
             )

display(traffic_df)

# COMMAND ----------

# DBTITLE 0,--i18n-0ef1149d-9690-49a3-b717-ae2c38a166ed
# MAGIC %md
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.functions import round

expected1 = [(620096.0, 1049.2318), (4026578.5, 986.1814), (1200591.0, 1067.192), (2322856.0, 1093.1087), (826921.0, 1086.6242), (404911.0, 1091.4043)]
test_df = traffic_df.sort("traffic_source").select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result1 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-f5c20afb-2891-4fa2-8090-cca7e313354d
# MAGIC %md
# MAGIC
# MAGIC ### 2. Get top three traffic sources by total revenue
# MAGIC - Sort by **`total_rev`** in descending order
# MAGIC - Limit to first three rows

# COMMAND ----------

# ANSWER
top_traffic_df = traffic_df.sort(col("total_rev").desc()).limit(3)
display(top_traffic_df)

# COMMAND ----------

# DBTITLE 0,--i18n-2ef23b0a-fc13-48ca-b1f3-9bc425023024
# MAGIC %md
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

expected2 = [(4026578.5, 986.1814), (2322856.0, 1093.1087), (1200591.0, 1067.192)]
test_df = top_traffic_df.select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result2 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected2 == result2)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-04399ae3-d2ab-4ada-9a51-9f8cc21cc45a
# MAGIC %md
# MAGIC
# MAGIC ### 3. Limit revenue columns to two decimal places
# MAGIC - Modify columns **`avg_rev`** and **`total_rev`** to contain numbers with two decimal places
# MAGIC   - Use **`withColumn()`** with the same names to replace these columns
# MAGIC   - To limit to two decimal places, multiply each column by 100, cast to long, and then divide by 100

# COMMAND ----------

# ANSWER
final_df = (top_traffic_df
            .withColumn("avg_rev", (col("avg_rev") * 100).cast("long") / 100)
            .withColumn("total_rev", (col("total_rev") * 100).cast("long") / 100)
           )

display(final_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d28b2d3a-6db6-4ba0-8a2c-a773635a69a4
# MAGIC %md
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**

# COMMAND ----------

expected3 = [(4026578.5, 986.18), (2322856.0, 1093.1), (1200591.0, 1067.19)]
result3 = [(row.total_rev, row.avg_rev) for row in final_df.collect()]

assert(expected3 == result3)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-4e2d3b62-bee6-497e-b6af-44064f759451
# MAGIC %md
# MAGIC ### 4. Bonus: Rewrite using a built-in math function
# MAGIC Find a built-in math function that rounds to a specified number of decimal places

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import round

bonus_df = (top_traffic_df
            .withColumn("avg_rev", round("avg_rev", 2))
            .withColumn("total_rev", round("total_rev", 2))
           )

display(bonus_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6514f89e-1920-4804-96e4-a73998026023
# MAGIC %md
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

expected4 = [(4026578.5, 986.18), (2322856.0, 1093.11), (1200591.0, 1067.19)]
result4 = [(row.total_rev, row.avg_rev) for row in bonus_df.collect()]

assert(expected4 == result4)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-8f19689f-4cf7-4031-bc4c-eb2ece7cb56d
# MAGIC %md
# MAGIC
# MAGIC ### 5. Chain all the steps above

# COMMAND ----------

# ANSWER
chain_df = (df
            .groupBy("traffic_source")
            .agg(sum(col("revenue")).alias("total_rev"),
                 avg(col("revenue")).alias("avg_rev"))
            .sort(col("total_rev").desc())
            .limit(3)
            .withColumn("avg_rev", round("avg_rev", 2))
            .withColumn("total_rev", round("total_rev", 2))
           )

display(chain_df)

# COMMAND ----------

# DBTITLE 0,--i18n-53c0b070-d2bc-45e9-a3ea-da25a375d6f3
# MAGIC %md
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**

# COMMAND ----------

expected5 = [(4026578.5, 986.18), (2322856.0, 1093.11), (1200591.0, 1067.19)]
result5 = [(row.total_rev, row.avg_rev) for row in chain_df.collect()]

assert(expected5 == result5)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-f8095ac2-c3cf-4bbb-b20b-eed1891489e0
# MAGIC %md
# MAGIC
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
