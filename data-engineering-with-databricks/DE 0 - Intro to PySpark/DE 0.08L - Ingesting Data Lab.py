# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-b24dbbe7-205a-4a6a-8b35-ef14ee51f01c
# MAGIC %md
# MAGIC # データのインジェストラボ
# MAGIC
# MAGIC 製品データを含むCSVファイルを読み込みます。
# MAGIC
# MAGIC ##### タスク
# MAGIC 1. スキーマを推測して読み込む
# MAGIC 2. ユーザー定義のスキーマを使用して読み込む
# MAGIC 3. スキーマをDDL形式の文字列として読み込む
# MAGIC 4. デルタ形式を使用して書き込む

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.08L

# COMMAND ----------

# DBTITLE 0,--i18n-13d2d883-def7-49df-b634-910428adc5a2
# MAGIC %md
# MAGIC
# MAGIC ### 1. スキーマを推測して読み込む
# MAGIC - 変数 **`single_product_csv_file_path`** で指定されたファイルパスを使用して、DBUtilsメソッド **`fs.head`** を使用して最初のCSVファイルを表示します。
# MAGIC - 変数 **`products_csv_path`** で指定されたファイルパスからCSVファイルを読み込んで、 **`products_df`** を作成します。
# MAGIC   - オプションを設定して、最初の行をヘッダーとして使用し、スキーマを推測します。

# COMMAND ----------

# TODO

single_product_csv_file_path = f"{DA.paths.products_csv}/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
print(FILL_IN)

products_csv_path = DA.paths.products_csv
products_df = FILL_IN

products_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-3ca1ef04-e7a4-4e9b-a49e-adba3180d9a4
# MAGIC %md
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

assert(products_df.count() == 12)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-d7f8541b-1691-4565-af41-6c8f36454e95
# MAGIC %md
# MAGIC
# MAGIC ### 2. ユーザー定義のスキーマを使用して読み込む
# MAGIC 列名とデータ型を持つ **`StructType`** を作成してスキーマを定義します。

# COMMAND ----------

# TODO
user_defined_schema = FILL_IN

products_df2 = FILL_IN

# COMMAND ----------

# DBTITLE 0,--i18n-af9a4134-5b88-4c6b-a3bb-8a1ae7b00e53
# MAGIC %md
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

assert(user_defined_schema.fieldNames() == ["item_id", "name", "price"])
print("All test pass")

# COMMAND ----------

from pyspark.sql import Row

expected1 = Row(item_id="M_STAN_Q", name="Standard Queen Mattress", price=1045.0)
result1 = products_df2.first()

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-0a52c971-8cff-4d89-b9fb-375e1c48364d
# MAGIC %md
# MAGIC
# MAGIC ### 3. DDL形式の文字列を使用して読み込む

# COMMAND ----------

# TODO
ddl_schema = FILL_IN

products_df3 = FILL_IN

# COMMAND ----------

# DBTITLE 0,--i18n-733b1e61-b319-4da6-9626-3f806435eec5
# MAGIC %md
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**

# COMMAND ----------

assert(products_df3.count() == 12)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-0b7e2b59-3fb1-4179-9793-bddef0159c89
# MAGIC %md
# MAGIC
# MAGIC ### 4. Deltaに書き込む
# MAGIC  **`products_df`** を変数 **`products_output_path`** で提供されたファイルパスに書き込んでください。

# COMMAND ----------

# TODO
products_output_path = DA.paths.working_dir + "/delta/products"
products_df.FILL_IN

# COMMAND ----------

# DBTITLE 0,--i18n-fb47127c-018d-4da0-8d6d-8584771ccd64
# MAGIC %md
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

verify_files = dbutils.fs.ls(products_output_path)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == "_delta_log/":
        verify_delta_format = True
    elif f.name.endswith(".parquet"):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files > 0, "No data written"
del verify_files, verify_delta_format, verify_num_data_files
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-42fb4bd4-287f-4863-b6ea-f635f315d8ec
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
