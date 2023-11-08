# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-da4e23df-1911-4f58-9030-65da697d7b61
# MAGIC %md
# MAGIC # Spark SQL ラボ
# MAGIC
# MAGIC ##### タスク
# MAGIC 1. **`events`** テーブルからDataFrameを作成する
# MAGIC 1. DataFrameを表示し、そのスキーマを確認する
# MAGIC 1. フィルターとソートの変換を適用して **`macOS`** イベントを処理する
# MAGIC 1. 結果をカウントし、最初の5行を取得する
# MAGIC 1. SQLクエリを使用して同じDataFrameを作成する
# MAGIC
# MAGIC ##### メソッド
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">SparkSession</a>: **`sql`**, **`table`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> 変換: **`select`**, **`where`**, **`orderBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a> アクション: **`select`**, **`count`**, **`take`**
# MAGIC - その他の <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> メソッド: **`printSchema`**, **`schema`**, **`createOrReplaceTempView`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.02L

# COMMAND ----------

# DBTITLE 0,--i18n-e0f3f405-8c97-46d1-8550-fb8ff14e5bd6
# MAGIC %md
# MAGIC
# MAGIC ### 1. **`events`** テーブルからDataFrameを作成する
# MAGIC - SparkSessionを使用して **`events`** テーブルからDataFrameを作成します。
# MAGIC

# COMMAND ----------

# TODO
events_df = FILL_IN

# COMMAND ----------

# DBTITLE 0,--i18n-fb5458a0-b475-4d77-b06b-63bb9a18d586
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ### 2. DataFrameを表示し、スキーマを確認する
# MAGIC - 上記のメソッドを使用してDataFrameの内容とスキーマを確認します。
# MAGIC

# COMMAND ----------

# TODO

# COMMAND ----------

# DBTITLE 0,--i18n-76adfcb2-f182-485c-becd-9e569d4148b6
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ### 3. **`macOS`** イベントをフィルターおよびソートする変換を適用する
# MAGIC - **`device`** が **`macOS`** である行をフィルターします。
# MAGIC - **`event_timestamp`** で行をソートします。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> フィルターSQL式で単引用符と二重引用符を使用します
# MAGIC

# COMMAND ----------

# TODO
mac_df = (events_df
          .FILL_IN
         )

# COMMAND ----------

# DBTITLE 0,--i18n-81f8748d-a154-468b-b02e-ef1a1b6b2ba8
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ### 4. 結果をカウントし、最初の5行を取得する
# MAGIC - DataFrameアクションを使用して行をカウントし、取得します。
# MAGIC

# COMMAND ----------

# TODO
num_rows = mac_df.FILL_IN
rows = mac_df.FILL_IN

# COMMAND ----------

# DBTITLE 0,--i18n-4e340689-5d23-499a-9cd2-92509a646de6
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC **4.1: 作業を確認する**
# MAGIC

# COMMAND ----------

from pyspark.sql import Row

assert(num_rows == 97150)
assert(len(rows) == 5)
assert(type(rows[0]) == Row)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-cbb03650-db3b-42b3-96ee-54ea9b287ab5
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ### 5. SQLクエリを使用して同じDataFrameを作成する
# MAGIC - SparkSessionを使用して **`events`** テーブルでSQLクエリを実行します。
# MAGIC - 以前に使用した同じフィルターおよびソートクエリを書くためにSQLコマンドを使用します。
# MAGIC

# COMMAND ----------

# TODO
mac_sql_df = spark.FILL_IN

display(mac_sql_df)

# COMMAND ----------

# DBTITLE 0,--i18n-1d203e4e-e835-4778-a245-daf30cc9f4bc
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC **5.1: 作業を確認する**
# MAGIC - **`device`** 列には **`macOS`** の値のみが表示されるはずです。
# MAGIC - 5行目はタイムスタンプ **`1592539226602157`** のイベントであるはずです。
# MAGIC

# COMMAND ----------

verify_rows = mac_sql_df.take(5)
assert (mac_sql_df.select("device").distinct().count() == 1 and len(verify_rows) == 5 and verify_rows[0]['device'] == "macOS"), "Incorrect filter condition"
assert (verify_rows[4]['event_timestamp'] == 1592540419446946), "Incorrect sorting"
del verify_rows
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-5b3843b3-e615-4dc6-aec4-c8ce4d684464
# MAGIC
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
