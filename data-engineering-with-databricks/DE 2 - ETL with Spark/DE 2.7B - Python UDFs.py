# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Pythonユーザー定義関数
# MAGIC
# MAGIC ##### 目標
# MAGIC 1. 関数を定義する
# MAGIC 1. UDFを作成および適用する
# MAGIC 1. Pythonデコレータ構文を使用してUDFを作成および登録する
# MAGIC 1. Pandas（ベクトル化）UDFを作成および適用する
# MAGIC
# MAGIC ##### メソッド
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html" target="_blank">Python UDFデコレータ</a>: **`@udf`**
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.pandas_udf.html" target="_blank">Pandas UDFデコレータ</a>: **`@pandas_udf`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.7B

# COMMAND ----------

# DBTITLE 0,--i18n-1e94c419-dd84-4f8d-917a-019b15fc6700
# MAGIC %md
# MAGIC
# MAGIC ### ユーザー定義関数（UDF）
# MAGIC カスタムカラム変換関数
# MAGIC
# MAGIC - Catalyst Optimizerによって最適化されません
# MAGIC - 関数はシリアル化され、エグゼキュータに送信されます
# MAGIC - 行データはSparkのネイティブバイナリ形式からデシリアライズされ、UDFに渡すために使用され、結果はSparkのネイティブ形式に再シリアル化されます
# MAGIC - Python UDFの場合、各ワーカーノードで実行されているPythonインタープリタとエグゼキュータ間で追加
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-4d1eb639-23fb-42fa-9b62-c407a0ccde2d
# MAGIC %md
# MAGIC
# MAGIC このデモでは、販売データを使用します。

# COMMAND ----------

sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-05043672-b02a-4194-ba44-75d544f6af07
# MAGIC %md
# MAGIC
# MAGIC ### 関数を定義
# MAGIC
# MAGIC **`email`** フィールドから文字列の最初の文字を取得する関数（ドライバ上で）を定義します。

# COMMAND ----------

def first_letter_function(email):
    return email[0]

first_letter_function("annagray@kaufman.com")

# COMMAND ----------

# DBTITLE 0,--i18n-17f25aa9-c20f-41da-bac5-95ebb413dcd4
# MAGIC %md
# MAGIC
# MAGIC ### UDFを作成および適用
# MAGIC 関数をUDFとして登録します。これにより、関数がシリアル化され、エグゼキュータに送信され、DataFrameのレコードを変換できるようになります。

# COMMAND ----------

first_letter_udf = udf(first_letter_function)

# COMMAND ----------

# DBTITLE 0,--i18n-75abb6ee-291b-412f-919d-be646cf1a580
# MAGIC %md
# MAGIC
# MAGIC **`email`** カラムにUDFを適用します。

# COMMAND ----------

from pyspark.sql.functions import col

display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-26f93012-a994-4b6a-985e-01720dbecc25
# MAGIC %md
# MAGIC
# MAGIC ### デコレータ構文の使用（Pythonのみ）
# MAGIC
# MAGIC また、Pythonデコレータ構文を使用してUDFを定義および登録することもできます。 **`@udf`** デコレータのパラメータは、関数が返す列のデータ型です。
# MAGIC
# MAGIC もはやローカルのPython関数を呼び出すことはできなくなります（つまり、 **`first_letter_udf("annagray@kaufman.com")`** は動作しません）。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> この例では、<a href="https://docs.python.org/3/library/typing.html" target="_blank">Python型ヒント</a>も使用されています。これはPython 3.5で導入されました。この例では

# COMMAND ----------

# Our input/output is a string
@udf("string")
def first_letter_udf(email: str) -> str:
    return email[0]

# COMMAND ----------

# DBTITLE 0,--i18n-4d628fe1-2d94-4d86-888d-7b9df4107dba
# MAGIC %md
# MAGIC
# MAGIC そして、ここでデコレータUDFを使用しましょう。

# COMMAND ----------

from pyspark.sql.functions import col

sales_df = spark.table("sales")
display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-3ae354c0-0b10-4e8c-8cf6-da68e8fba9f2
# MAGIC %md
# MAGIC
# MAGIC ### Pandas/Vectorized UDFs
# MAGIC
# MAGIC Pandas UDFsはPythonで利用可能で、UDFの効率を向上させるために使用されます。Pandas UDFsは計算を高速化するためにApache Arrowを利用しています。
# MAGIC
# MAGIC * <a href="https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html" target="_blank">ブログ記事</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html?highlight=arrow" target="_blank">ドキュメンテーション</a>
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" alt="Benchmark" width ="500" height="1500">
# MAGIC
# MAGIC ユーザー定義関数は以下を使用して実行されます：
# MAGIC * <a href="https://arrow.apache.org/" target="_blank">Apache Arrow</a>、JVMとPythonプロセス間でデータを効率的に転送するためにSparkで使用されるインメモリ列データフォーマット。これにより、（デ）シリアライゼーションコストがほぼゼロとなります。
# MAGIC * 関数内部でPandasを使用して、PandasのインスタンスとAPIと連携します。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> Spark 3.0以降、Pandas UDFを常にPython型ヒントを使用して定義する必要があります。
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

# We have a string input/output
@pandas_udf("string")
def vectorized_udf(email: pd.Series) -> pd.Series:
    return email.str[0]

# Alternatively
# def vectorized_udf(email: pd.Series) -> pd.Series:
#     return email.str[0]
# vectorized_udf = pandas_udf(vectorized_udf, "string")

# COMMAND ----------

display(sales_df.select(vectorized_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-9a2fb1b1-8060-4e50-a759-f30dc73ce1a1
# MAGIC %md
# MAGIC
# MAGIC これらのPandas UDFをSQLの名前空間に登録できます。

# COMMAND ----------

spark.udf.register("sql_vectorized_udf", vectorized_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the Pandas UDF from SQL
# MAGIC SELECT sql_vectorized_udf(email) AS firstLetter FROM sales

# COMMAND ----------

# DBTITLE 0,--i18n-5e506b8d-a488-4373-af9a-9ebb14834b1b
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
