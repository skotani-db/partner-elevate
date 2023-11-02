# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-c52349f9-afd2-4532-804b-2d50a67839fa
# MAGIC %md
# MAGIC # Abandoned Carts Lab
# MAGIC
# MAGIC 未購入のカートアイテムを取得するためのタスク:
# MAGIC 1. 取引から変換されたユーザーのメールアドレスを取得します。
# MAGIC 2. ユーザーIDとメールアドレスを結合します。
# MAGIC 3. 各ユーザーのカートアイテム履歴を取得します。
# MAGIC 4. カートアイテム履歴をメールアドレスと結合します。
# MAGIC 5. 未購入のカートアイテムを持つメールアドレスをフィルタリングします。
# MAGIC
# MAGIC ##### メソッド
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join" target="_blank">DataFrame</a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">組み込み関数</a>: **`collect_set`**, **`explode`**, **`lit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>: **`fill`**

# COMMAND ----------

# DBTITLE 0,--i18n-f1f59329-e456-4268-836a-898d3736f378
# MAGIC %md
# MAGIC ### セットアップ
# MAGIC 以下のセルを実行して、**`sales_df`**、**`users_df`**、および**`events_df`**のDataFrameを作成します。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.12L

# COMMAND ----------

# sale transactions at BedBricks
sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# user IDs and emails at BedBricks
users_df = spark.table("users")
display(users_df)

# COMMAND ----------

# events logged on the BedBricks website
events_df = spark.table("events")
display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-c2065783-4c56-4d12-bdf8-2e0fce22016f
# MAGIC %md
# MAGIC
# MAGIC ### 1: トランザクションから変換されたユーザーのメールを取得
# MAGIC - **`sales_df`** 内の **`email`** 列を選択し、重複を削除します
# MAGIC - 新しい列 **`converted`** を追加し、すべての行に値 **`True`** を設定します
# MAGIC
# MAGIC 結果を **`converted_users_df`** として保存します。

# COMMAND ----------

# TODO
from pyspark.sql.functions import *

converted_users_df = (sales_df.FILL_IN
                     )
display(converted_users_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4becd415-94d5-4e58-a995-d17ef1be87d0
# MAGIC %md
# MAGIC
# MAGIC #### 1.1: Check Your Work
# MAGIC
# MAGIC 以下のセルを実行して、あなたの解決策が正常に動作するか確認してください:

# COMMAND ----------

expected_columns = ["email", "converted"]

expected_count = 10510

assert converted_users_df.columns == expected_columns, "converted_users_df does not have the correct columns"

assert converted_users_df.count() == expected_count, "converted_users_df does not have the correct number of rows"

assert converted_users_df.select(col("converted")).first()[0] == True, "converted column not correct"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-72c8bd3a-58ae-4c30-8e3e-007d9608aea3
# MAGIC %md
# MAGIC ### 2: ユーザーIDとメールアドレスの結合
# MAGIC - **`converted_users_df`** と **`users_df`** を **`email`** フィールドで外部結合します
# MAGIC - **`email`** が null でないユーザーをフィルタリングします
# MAGIC - **`converted`** フィールドの null 値を **`False`** で埋めます
# MAGIC
# MAGIC 結果を **`conversions_df`** として保存します。

# COMMAND ----------

# TODO
conversions_df = (users_df.FILL_IN
                 )
display(conversions_df)

# COMMAND ----------

# DBTITLE 0,--i18n-48691094-e17f-405d-8f91-286a42ce55d7
# MAGIC %md
# MAGIC
# MAGIC #### 2.1: Check Your Work
# MAGIC
# MAGIC 以下のセルを実行して、あなたの解決策が正常に動作するか確認してください:

# COMMAND ----------

expected_columns = ['email', 'user_id', 'user_first_touch_timestamp', 'updated', 'converted']

expected_count = 38939

expected_false_count = 28429

assert conversions_df.columns == expected_columns, "Columns are not correct"

assert conversions_df.filter(col("email").isNull()).count() == 0, "Email column contains null"

assert conversions_df.count() == expected_count, "There is an incorrect number of rows"

assert conversions_df.filter(col("converted") == False).count() == expected_false_count, "There is an incorrect number of false entries in converted column"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-8a92bfe3-cad0-40af-a283-3241b810fc20
# MAGIC %md
# MAGIC ### 3: 各ユーザーのカートアイテム履歴を取得
# MAGIC - **`events_df`** の **`items`** フィールドを展開し、結果を既存の **`items`** フィールドに置き換えます
# MAGIC - **`user_id`** でグループ化します
# MAGIC   - 各ユーザーのすべての **`items.item_id`** オブジェクトのセットを収集し、カラムを "cart" にエイリアスします
# MAGIC
# MAGIC 結果を **`carts_df`** として保存します。

# COMMAND ----------

# TODO
carts_df = (events_df.FILL_IN
)
display(carts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d0ed8434-7016-44c0-a865-4b55a5001194
# MAGIC %md
# MAGIC
# MAGIC #### 3.1: Check Your Work
# MAGIC
# MAGIC 以下のセルを実行して、あなたの解決策が正常に動作するか確認してください:

# COMMAND ----------

expected_columns = ["user_id", "cart"]

expected_count = 24574

assert carts_df.columns == expected_columns, "Incorrect columns"

assert carts_df.count() == expected_count, "Incorrect number of rows"

assert carts_df.select(col("user_id")).drop_duplicates().count() == expected_count, "Duplicate user_ids present"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-97b01bbb-ada0-4c4f-a253-7c578edadaf9
# MAGIC %md
# MAGIC ### 4: カートアイテム履歴をメールと結合
# MAGIC - **`user_id`** フィールドを基に **`conversions_df`** と **`carts_df`** で左結合を実行します
# MAGIC
# MAGIC 結果を **`email_carts_df`** として保存します。

# COMMAND ----------

# TODO
email_carts_df = conversions_df.FILL_IN
display(email_carts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-0cf80000-eb4c-4f0f-a3f8-7e938b99f1ef
# MAGIC %md
# MAGIC
# MAGIC #### 4.1: Check Your Work
# MAGIC
# MAGIC 以下のセルを実行して、あなたの解決策が正常に動作するか確認してください:

# COMMAND ----------

email_carts_df.filter(col("cart").isNull()).count()

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "updated", "converted", "cart"]

expected_count = 38939

expected_cart_null_count = 19671

assert email_carts_df.columns == expected_columns, "Columns do not match"

assert email_carts_df.count() == expected_count, "Counts do not match"

assert email_carts_df.filter(col("cart").isNull()).count() == expected_cart_null_count, "Cart null counts incorrect from join"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-380e3eda-3543-4f67-ae11-b883e5201dba
# MAGIC %md
# MAGIC ### 5: 放棄されたカートアイテムを持つメールをフィルタリング
# MAGIC - **`converted`** が False のユーザーを **`email_carts_df`** からフィルタリングします
# MAGIC - ヌルでないカートを持つユーザーをフィルタリングします
# MAGIC
# MAGIC 結果を **`abandoned_carts_df`** として保存します。

# COMMAND ----------

# TODO
abandoned_carts_df = (email_carts_df.FILL_IN
)
display(abandoned_carts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-05ff2599-c1e6-404f-a38b-262fb0a055fa
# MAGIC %md
# MAGIC
# MAGIC #### 5.1: Check Your Work
# MAGIC
# MAGIC 以下のセルを実行して、あなたの解決策が正常に動作するか確認してください:

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "updated", "converted", "cart"]

expected_count = 10212

assert abandoned_carts_df.columns == expected_columns, "Columns do not match"

assert abandoned_carts_df.count() == expected_count, "Counts do not match"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-e2f48480-5c42-490a-9f14-9b92d29a9823
# MAGIC %md
# MAGIC ### 6: ボーナスアクティビティ
# MAGIC 製品ごとの放棄されたカートアイテムの数をプロットする

# COMMAND ----------

# TODO
abandoned_items_df = (abandoned_carts_df.FILL_IN
                     )
display(abandoned_items_df)

# COMMAND ----------

# DBTITLE 0,--i18n-a08b8c26-6a94-4a20-a096-e74721824eac
# MAGIC %md
# MAGIC
# MAGIC #### 6.1: Check Your Work
# MAGIC
# MAGIC 以下のセルを実行して、あなたの解決策が正常に動作するか確認してください:

# COMMAND ----------

expected_columns = ["items", "count"]

expected_count = 12

assert abandoned_items_df.count() == expected_count, "Counts do not match"

assert abandoned_items_df.columns == expected_columns, "Columns do not match"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-f2f44609-5139-465a-ad7d-d87f3f06a380
# MAGIC %md
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
