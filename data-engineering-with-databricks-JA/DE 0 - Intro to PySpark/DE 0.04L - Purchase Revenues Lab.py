# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-5b46ceba-8f87-4062-91cc-6f02f3303258
# MAGIC %md
# MAGIC # 購入収益ラボ
# MAGIC
# MAGIC 購入収益のあるイベントのデータセットを準備します。
# MAGIC
# MAGIC ##### タスク
# MAGIC 1. 各イベントの購入収益を抽出する
# MAGIC 2. 収益がnullでないイベントをフィルタリングする
# MAGIC 3. 収益を持っているイベントのタイプを確認する
# MAGIC 4. 不要な列を削除する
# MAGIC
# MAGIC ##### メソッド
# MAGIC - DataFrame: **`select`**, **`drop`**, **`withColumn`**, **`filter`**, **`dropDuplicates`**
# MAGIC - Column: **`isNotNull`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.04L

# COMMAND ----------

events_df = spark.table("events")
display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-412840ac-10d6-473e-a3ea-8e9e92446b80
# MAGIC %md
# MAGIC
# MAGIC ### 1. 各イベントの購入収益を抽出する
# MAGIC **`ecommerce.purchase_revenue_in_usd`** を抽出して新しい列 **`revenue`** を追加します。

# COMMAND ----------

# TODO
revenue_df = events_df.FILL_IN
display(revenue_df)

# COMMAND ----------

# DBTITLE 0,--i18n-66dfc9f4-0a59-482e-a743-cfdbc897aee8
# MAGIC %md
# MAGIC
# MAGIC **1.1: 作業を確認する**

# COMMAND ----------

from pyspark.sql.functions import col
expected1 = [4351.5, 4044.0, 3985.0, 3946.5, 3885.0, 3590.0, 3490.0, 3451.5, 3406.5, 3385.0]
result1 = [row.revenue for row in revenue_df.sort(col("revenue").desc_nulls_last()).limit(10).collect()]
print(result1)
assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-cb49af43-880a-4834-be9c-62f65581e67a
# MAGIC %md
# MAGIC
# MAGIC ### 2. 収益がnullでないイベントをフィルタリングする
# MAGIC **`revenue`** が **`null`** でないレコードをフィルタリングします。

# COMMAND ----------

# TODO
purchases_df = revenue_df.FILL_IN
display(purchases_df)

# COMMAND ----------

# DBTITLE 0,--i18n-3363869f-e2f4-4ec6-9200-9919dc38582b
# MAGIC %md
# MAGIC
# MAGIC **2.1: 作業を確認する**

# COMMAND ----------

assert purchases_df.filter(col("revenue").isNull()).count() == 0, "Nulls in 'revenue' column"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-6dd8d228-809d-4a3b-8aba-60da65c53f1c
# MAGIC %md
# MAGIC
# MAGIC ### 3. 収益を持つイベントのタイプを確認する
# MAGIC  **`purchases_df`** 内の一意の **`event_name`** 値を2つの方法のいずれかで見つけます：
# MAGIC - "event_name"を選択し、異なるレコードを取得する
# MAGIC - "event_name"のみに基づいて重複レコードを削除する
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> 収益に関連するイベントは1つだけです

# COMMAND ----------

# TODO
distinct_df = purchases_df.FILL_IN
display(distinct_df)

# COMMAND ----------

# DBTITLE 0,--i18n-f0d53260-4525-4942-b901-ce351f55d4c9
# MAGIC %md
# MAGIC ### 4. 不要な列を削除する
# MAGIC イベントタイプは1つだけなので、 **`purchases_df`** から **`event_name`** を削除します。

# COMMAND ----------

# TODO
final_df = purchases_df.FILL_IN
display(final_df)

# COMMAND ----------

# DBTITLE 0,--i18n-8ea4b4df-c55e-4015-95ee-1caccafa44d6
# MAGIC %md
# MAGIC
# MAGIC **4.1: 作業を確認する**

# COMMAND ----------

expected_columns = {"device", "ecommerce", "event_previous_timestamp", "event_timestamp",
                    "geo", "items", "revenue", "traffic_source",
                    "user_first_touch_timestamp", "user_id"}
assert(set(final_df.columns) == expected_columns)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-ed143b89-079a-44e9-87f3-9c8d242f09d2
# MAGIC %md
# MAGIC
# MAGIC ### 5. ステップ3を除いて上記のすべてのステップを連鎖させる

# COMMAND ----------

# TODO
final_df = (events_df
  .FILL_IN
)

display(final_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d7b35e13-8c38-4e17-b676-2146b64045fe
# MAGIC %md
# MAGIC
# MAGIC **5.1: 作業を確認する**

# COMMAND ----------

assert(final_df.count() == 9056)
print("All test pass")

# COMMAND ----------

expected_columns = {"device", "ecommerce", "event_previous_timestamp", "event_timestamp",
                    "geo", "items", "revenue", "traffic_source",
                    "user_first_touch_timestamp", "user_id"}
assert(set(final_df.columns) == expected_columns)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-03e7e278-385e-4afe-8268-229a1984a654
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
