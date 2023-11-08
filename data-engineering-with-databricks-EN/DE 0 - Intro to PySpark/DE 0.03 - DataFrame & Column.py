# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-ab2602b5-4183-4f33-8063-cfc03fcb1425
# MAGIC %md
# MAGIC # DataFrame & Column
# MAGIC ##### Objectives
# MAGIC 1. Construct columns
# MAGIC 1. Subset columns
# MAGIC 1. Add or replace columns
# MAGIC 1. Subset rows
# MAGIC 1. Sort rows
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`select`**, **`selectExpr`**, **`drop`**, **`withColumn`**, **`withColumnRenamed`**, **`filter`**, **`distinct`**, **`limit`**, **`sort`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`alias`**, **`isin`**, **`cast`**, **`isNotNull`**, **`desc`**, operators

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.03

# COMMAND ----------

# DBTITLE 0,--i18n-ef990348-e991-4edb-bf45-84de46a34759
# MAGIC %md
# MAGIC
# MAGIC Let's use the BedBricks events dataset.

# COMMAND ----------

events_df = spark.table("events")
display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4ea9a278-1eb6-45ad-9f96-34e0fd0da553
# MAGIC %md
# MAGIC
# MAGIC ## Column Expressions
# MAGIC
# MAGIC A <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a> is a logical construction that will be computed based on the data in a DataFrame using an expression
# MAGIC
# MAGIC Construct a new Column based on existing columns in a DataFrame

# COMMAND ----------

from pyspark.sql.functions import col

print(events_df.device)
print(events_df["device"])
print(col("device"))

# COMMAND ----------

# DBTITLE 0,--i18n-d87b8303-8f78-416e-99b0-b037caf2107a
# MAGIC %md
# MAGIC Scala supports an additional syntax for creating a new Column based on existing columns in a DataFrame

# COMMAND ----------

# MAGIC %scala
# MAGIC $"device"

# COMMAND ----------

# DBTITLE 0,--i18n-64238a77-0877-4bd4-af46-a9a8bd4763c6
# MAGIC %md
# MAGIC
# MAGIC ### Column Operators and Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | \*, + , <, >= | Math and comparison operators |
# MAGIC | ==, != | Equality and inequality tests (Scala operators are **`===`** and **`=!=`**) |
# MAGIC | alias | Gives the column an alias |
# MAGIC | cast, astype | Casts the column to a different data type |
# MAGIC | isNull, isNotNull, isNan | Is null, is not null, is NaN |
# MAGIC | asc, desc | Returns a sort expression based on ascending/descending order of the column |

# COMMAND ----------

# DBTITLE 0,--i18n-6d68007e-3dbf-4f18-bde4-6990299ef086
# MAGIC %md
# MAGIC
# MAGIC Create complex expressions with existing columns, operators, and methods.

# COMMAND ----------

col("ecommerce.purchase_revenue_in_usd") + col("ecommerce.total_item_quantity")
col("event_timestamp").desc()
(col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

# COMMAND ----------

# DBTITLE 0,--i18n-7c1c0688-8f9f-4247-b8b8-bb869414b276
# MAGIC %md
# MAGIC Here's an example of using these column expressions in the context of a DataFrame

# COMMAND ----------

rev_df = (events_df
         .filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
         .withColumn("purchase_revenue", (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int"))
         .withColumn("avg_purchase_revenue", col("ecommerce.purchase_revenue_in_usd") / col("ecommerce.total_item_quantity"))
         .sort(col("avg_purchase_revenue").desc())
        )

display(rev_df)

# COMMAND ----------

# DBTITLE 0,--i18n-7ba60230-ecd3-49dd-a4c8-d964addc6692
# MAGIC %md
# MAGIC
# MAGIC ## DataFrame Transformation Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`limit`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |

# COMMAND ----------

# DBTITLE 0,--i18n-3e95eb92-30e4-44aa-8ee0-46de94c2855e
# MAGIC %md
# MAGIC
# MAGIC ### Subset columns
# MAGIC Use DataFrame transformations to subset columns

# COMMAND ----------

# DBTITLE 0,--i18n-987cfd99-8e06-447f-b1c7-5f104cd5ed2f
# MAGIC %md
# MAGIC
# MAGIC #### **`select()`**
# MAGIC Selects a list of columns or column based expressions

# COMMAND ----------

devices_df = events_df.select("user_id", "device")
display(devices_df)

# COMMAND ----------

from pyspark.sql.functions import col

locations_df = events_df.select(
    "user_id", 
    col("geo.city").alias("city"), 
    col("geo.state").alias("state")
)
display(locations_df)

# COMMAND ----------

# DBTITLE 0,--i18n-8d556f84-bfcd-436a-a3dd-893143ce620e
# MAGIC %md
# MAGIC
# MAGIC #### **`selectExpr()`**
# MAGIC Selects a list of SQL expressions

# COMMAND ----------

apple_df = events_df.selectExpr("user_id", "device in ('macOS', 'iOS') as apple_user")
display(apple_df)

# COMMAND ----------

# DBTITLE 0,--i18n-452f7fb3-3866-4835-827f-6d359f364046
# MAGIC %md
# MAGIC
# MAGIC #### **`drop()`**
# MAGIC Returns a new DataFrame after dropping the given column, specified as a string or Column object
# MAGIC
# MAGIC Use strings to specify multiple columns

# COMMAND ----------

anonymous_df = events_df.drop("user_id", "geo", "device")
display(anonymous_df)

# COMMAND ----------

no_sales_df = events_df.drop(col("ecommerce"))
display(no_sales_df)


# COMMAND ----------

# DBTITLE 0,--i18n-b11609a3-11d5-453b-b713-15131b277066
# MAGIC %md
# MAGIC
# MAGIC ### Add or replace columns
# MAGIC Use DataFrame transformations to add or replace columns

# COMMAND ----------

# DBTITLE 0,--i18n-f29a47d9-9567-40e5-910b-73c640cc61ca
# MAGIC %md
# MAGIC
# MAGIC #### **`withColumn()`**
# MAGIC Returns a new DataFrame by adding a column or replacing an existing column that has the same name.

# COMMAND ----------

mobile_df = events_df.withColumn("mobile", col("device").isin("iOS", "Android"))
display(mobile_df)

# COMMAND ----------

purchase_quantity_df = events_df.withColumn("purchase_quantity", col("ecommerce.total_item_quantity").cast("int"))
purchase_quantity_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-969c0d9f-202f-405a-8c66-ef29076b48fc
# MAGIC %md
# MAGIC
# MAGIC #### **`withColumnRenamed()`**
# MAGIC Returns a new DataFrame with a column renamed.

# COMMAND ----------

location_df = events_df.withColumnRenamed("geo", "location")
display(location_df)

# COMMAND ----------

# DBTITLE 0,--i18n-23b0a9ef-58d5-4973-a610-93068a998d5e
# MAGIC %md
# MAGIC
# MAGIC ### Subset Rows
# MAGIC Use DataFrame transformations to subset rows

# COMMAND ----------

# DBTITLE 0,--i18n-4ada6444-7345-41f7-aaa2-1de2d729483f
# MAGIC %md
# MAGIC
# MAGIC #### **`filter()`**
# MAGIC Filters rows using the given SQL expression or column based condition.
# MAGIC
# MAGIC ##### Alias: **`where`**

# COMMAND ----------

purchases_df = events_df.filter("ecommerce.total_item_quantity > 0")
display(purchases_df)

# COMMAND ----------

revenue_df = events_df.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
display(revenue_df)

# COMMAND ----------

android_df = events_df.filter((col("traffic_source") != "direct") & (col("device") == "Android"))
display(android_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4d6a79eb-3989-43e1-8c28-5b976a513f5f
# MAGIC %md
# MAGIC
# MAGIC #### **`dropDuplicates()`**
# MAGIC Returns a new DataFrame with duplicate rows removed, optionally considering only a subset of columns.
# MAGIC
# MAGIC ##### Alias: **`distinct`**

# COMMAND ----------

display(events_df.distinct())

# COMMAND ----------

distinct_users_df = events_df.dropDuplicates(["user_id"])
display(distinct_users_df)

# COMMAND ----------

# DBTITLE 0,--i18n-433c57f4-ce40-48c9-8d04-d3a13c398082
# MAGIC %md
# MAGIC
# MAGIC #### **`limit()`**
# MAGIC Returns a new DataFrame by taking the first n rows.

# COMMAND ----------

limit_df = events_df.limit(100)
display(limit_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d4117305-e742-497e-964d-27a7b0c395cd
# MAGIC %md
# MAGIC
# MAGIC ### Sort rows
# MAGIC Use DataFrame transformations to sort rows

# COMMAND ----------

# DBTITLE 0,--i18n-16b3c7fe-b5f2-4564-9e8e-4f677777c50c
# MAGIC %md
# MAGIC
# MAGIC #### **`sort()`**
# MAGIC Returns a new DataFrame sorted by the given columns or expressions.
# MAGIC
# MAGIC ##### Alias: **`orderBy`**

# COMMAND ----------

increase_timestamps_df = events_df.sort("event_timestamp")
display(increase_timestamps_df)

# COMMAND ----------

decrease_timestamp_df = events_df.sort(col("event_timestamp").desc())
display(decrease_timestamp_df)

# COMMAND ----------

increase_sessions_df = events_df.orderBy(["user_first_touch_timestamp", "event_timestamp"])
display(increase_sessions_df)

# COMMAND ----------

decrease_sessions_df = events_df.sort(col("user_first_touch_timestamp").desc(), col("event_timestamp"))
display(decrease_sessions_df)

# COMMAND ----------

# DBTITLE 0,--i18n-555c663e-3f62-4478-9d76-c9ee090beca1
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
