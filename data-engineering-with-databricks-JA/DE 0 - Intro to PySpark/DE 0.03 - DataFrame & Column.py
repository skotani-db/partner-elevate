# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-ab2602b5-4183-4f33-8063-cfc03fcb1425
# MAGIC %md
# MAGIC # DataFrame & Column（データフレームと列）
# MAGIC ##### 目標
# MAGIC 1. 列を構築する
# MAGIC 1. 列をサブセットする
# MAGIC 1. 列を追加または置き換える
# MAGIC 1. 行をサブセットする
# MAGIC 1. 行をソートする
# MAGIC
# MAGIC ##### メソッド
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`select`**, **`selectExpr`**, **`drop`**, **`withColumn`**, **`withColumnRenamed`**, **`filter`**, **`distinct`**, **`limit`**, **`sort`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`alias`**, **`isin`**, **`cast`**, **`isNotNull`**, **`desc`**, 演算子
# MAGIC

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.03

# COMMAND ----------

# DBTITLE 0,--i18n-ef990348-e991-4edb-bf45-84de46a34759
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC BedBricksイベントデータセットを使用しましょう。
# MAGIC

# COMMAND ----------

events_df = spark.table("events")
display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4ea9a278-1eb6-45ad-9f96-34e0fd0da553
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ## カラム表現
# MAGIC
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>は、式を使用してDataFrame内のデータに基づいて計算される論理構造です。
# MAGIC
# MAGIC DataFrame内の既存の列に基づいて新しいColumnを構築します。
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

print(events_df.device)
print(events_df["device"])
print(col("device"))

# COMMAND ----------

# DBTITLE 0,--i18n-d87b8303-8f78-416e-99b0-b037caf2107a
# MAGIC
# MAGIC %md
# MAGIC Scalaは、DataFrame内の既存の列に基づいて新しいColumnを作成するための追加の構文をサポートしています。
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC $"device"

# COMMAND ----------

# DBTITLE 0,--i18n-64238a77-0877-4bd4-af46-a9a8bd4763c6
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ### 列演算子とメソッド
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | \*, + , <, >= | 数学および比較演算子 |
# MAGIC | ==, != | 等価および非等価テスト（Scalaの演算子は **`===`** および **`=!=`**）|
# MAGIC | alias | 列にエイリアスを与えます |
# MAGIC | cast, astype | 列を異なるデータ型にキャストします |
# MAGIC | isNull, isNotNull, isNan | null、非null、NaNの判断 |
# MAGIC | asc, desc | 列の昇順/降順に基づいてソート式を返します |
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-6d68007e-3dbf-4f18-bde4-6990299ef086
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC 既存の列、演算子、およびメソッドを使用して複雑な式を作成します。

# COMMAND ----------

col("ecommerce.purchase_revenue_in_usd") + col("ecommerce.total_item_quantity")
col("event_timestamp").desc()
(col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

# COMMAND ----------

# DBTITLE 0,--i18n-7c1c0688-8f9f-4247-b8b8-bb869414b276
# MAGIC %md
# MAGIC これは、DataFrameのコンテキストでこれらの列式を使用する例です。

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
# MAGIC ## DataFrame変換メソッド
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | **`select`** | 各要素に対して指定された式を計算することで新しいDataFrameを返します |
# MAGIC | **`drop`** | 列を削除した新しいDataFrameを返します |
# MAGIC | **`withColumnRenamed`** | 列の名前を変更した新しいDataFrameを返します |
# MAGIC | **`withColumn`** | 列を追加するか、同じ名前を持つ既存の列を置き換えることで新しいDataFrameを返します |
# MAGIC | **`filter`**, **`where`** | 指定された条件を使用して行をフィルタリングします |
# MAGIC | **`sort`**, **`orderBy`** | 指定された式でソートされた新しいDataFrameを返します |
# MAGIC | **`dropDuplicates`**, **`distinct`** | 重複した行を削除した新しいDataFrameを返します |
# MAGIC | **`limit`** | 最初のn行を取得することで新しいDataFrameを返します |
# MAGIC | **`groupBy`** | 指定された列を使用してDataFrameをグループ化し、それに対して集計を実行できるようにします |

# COMMAND ----------

# DBTITLE 0,--i18n-3e95eb92-30e4-44aa-8ee0-46de94c2855e
# MAGIC %md
# MAGIC
# MAGIC ### 列のサブセット
# MAGIC DataFrame変換を使用して列のサブセットを作成します

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
# MAGIC SQL式のリストを選択します

# COMMAND ----------

apple_df = events_df.selectExpr("user_id", "device in ('macOS', 'iOS') as apple_user")
display(apple_df)

# COMMAND ----------

# DBTITLE 0,--i18n-452f7fb3-3866-4835-827f-6d359f364046
# MAGIC %md
# MAGIC
# MAGIC #### **`drop()`**
# MAGIC 指定された列を削除した後の新しいDataFrameを返します。列は文字列またはColumnオブジェクトとして指定されます。
# MAGIC
# MAGIC 複数の列を指定するには、文字列を使用します。

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
# MAGIC ### 列を追加または置き換える
# MAGIC DataFrame変換を使用して列を追加または置き換えます

# COMMAND ----------

# DBTITLE 0,--i18n-f29a47d9-9567-40e5-910b-73c640cc61ca
# MAGIC %md
# MAGIC
# MAGIC #### **`withColumn()`**
# MAGIC 列を追加するか、同じ名前を持つ既存の列を置き換えることで新しいDataFrameを返します。

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
# MAGIC 列の名前を変更した新しいDataFrameを返します。

# COMMAND ----------

location_df = events_df.withColumnRenamed("geo", "location")
display(location_df)

# COMMAND ----------

# DBTITLE 0,--i18n-23b0a9ef-58d5-4973-a610-93068a998d5e
# MAGIC %md
# MAGIC
# MAGIC ### 行のサブセット
# MAGIC DataFrame変換を使用して行のサブセットを作成します

# COMMAND ----------

# DBTITLE 0,--i18n-4ada6444-7345-41f7-aaa2-1de2d729483f
# MAGIC %md
# MAGIC
# MAGIC #### **`filter()`**
# MAGIC 与えられたSQL式または列ベースの条件を使用して行をフィルタリングします。
# MAGIC
# MAGIC ##### エイリアス: **`where`**

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
# MAGIC 重複した行を削除した新しいDataFrameを返します。オプションで列のサブセットのみを考慮することができます。
# MAGIC
# MAGIC ##### エイリアス: **`distinct`**

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
# MAGIC 最初のn行を取得して新しいDataFrameを返します。

# COMMAND ----------

limit_df = events_df.limit(100)
display(limit_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d4117305-e742-497e-964d-27a7b0c395cd
# MAGIC %md
# MAGIC
# MAGIC ### 行をソートする
# MAGIC DataFrame変換を使用して行をソートします

# COMMAND ----------

# DBTITLE 0,--i18n-16b3c7fe-b5f2-4564-9e8e-4f677777c50c
# MAGIC %md
# MAGIC
# MAGIC #### **`sort()`**
# MAGIC 与えられた列または式でソートされた新しいDataFrameを返します。
# MAGIC
# MAGIC ##### エイリアス: **`orderBy`**

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
# MAGIC 以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除します。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
