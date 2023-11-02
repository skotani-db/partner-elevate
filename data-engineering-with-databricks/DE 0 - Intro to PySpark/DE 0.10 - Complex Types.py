# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-10b1d2c4-b58e-4a1c-a4be-29c3c07c7832
# MAGIC %md
# MAGIC
# MAGIC # コンプレックスタイプ
# MAGIC
# MAGIC コレクションと文字列を操作するための組み込み関数を探索します。
# MAGIC
# MAGIC ##### 目標
# MAGIC 1. 配列を処理するためのコレクション関数を適用する
# MAGIC 1. データフレームを結合する
# MAGIC
# MAGIC ##### メソッド
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>:**`union`**, **`unionByName`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">組み込み関数</a>:
# MAGIC   - 集計: **`collect_set`**
# MAGIC   - コレクション: **`array_contains`**, **`element_at`**, **`explode`**
# MAGIC   - 文字列: **`split`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.10

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("sales")

display(df)

# COMMAND ----------

# You will need this DataFrame for a later exercise
details_df = (df
              .withColumn("items", explode("items"))
              .select("email", "items.item_name")
              .withColumn("details", split(col("item_name"), " "))
             )
display(details_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4306b462-66db-488e-8106-66e1bbbd30d9
# MAGIC %md
# MAGIC
# MAGIC ### 文字列関数
# MAGIC
# MAGIC 以下は、文字列を操作するための組み込み関数のいくつかです。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | translate | src内の任意の文字をreplaceString内の文字で置き換えます |
# MAGIC | regexp_replace | regexpに一致する指定された文字列値のすべての部分文字列をrepで置き換えます |
# MAGIC | regexp_extract | 指定された文字列列からJava正規表現に一致する特定のグループを抽出します |
# MAGIC | ltrim | 指定された文字列列から前方の空白文字を削除します |
# MAGIC | lower | 文字列列を小文字に変換します |
# MAGIC | split | 指定されたパターンの一致を基準にstrを分割します |

# COMMAND ----------

# DBTITLE 0,--i18n-12dcf4bd-35e7-4316-b03f-ec076e9739c7
# MAGIC %md
# MAGIC 例えば、私たちが **`email`** 列を解析する必要があるとしましょう。私たちはドメインとハンドルを分割するために **`split`** 関数を使用します。

# COMMAND ----------

from pyspark.sql.functions import split

# COMMAND ----------

display(df.select(split(df.email, '@', 0).alias('email_handle')))

# COMMAND ----------

# DBTITLE 0,--i18n-4be5a98f-61e3-483b-b7a6-af4b671eb057
# MAGIC %md
# MAGIC
# MAGIC ### コレクション関数
# MAGIC
# MAGIC 配列を操作するための組み込み関数のいくつかは以下の通りです。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | array_contains | 配列がnullの場合はnullを、配列が指定した値を含む場合はtrueを、それ以外の場合はfalseを返します。 |
# MAGIC | element_at | 指定されたインデックスの配列要素を返します。配列要素は**1**から始まる番号で数えられます。 |
# MAGIC | explode | 与えられた配列またはマップ列の各要素に対して新しい行を作成します。 |
# MAGIC | collect_set | 重複した要素が削除されたオブジェクトのセットを返します。 |

# COMMAND ----------

mattress_df = (details_df
               .filter(array_contains(col("details"), "Mattress"))
               .withColumn("size", element_at(col("details"), 2)))
display(mattress_df)

# COMMAND ----------

# DBTITLE 0,--i18n-110c4036-291a-4ca8-a61c-835e2abb1ffc
# MAGIC %md
# MAGIC
# MAGIC ### 集計関数
# MAGIC
# MAGIC 以下は、通常GroupedDataから配列を作成するために使用できる組み込みの集計関数のいくつかです。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | collect_list | グループ内のすべての値からなる配列を返します。 |
# MAGIC | collect_set | グループ内のすべての一意の値からなる配列を返します。 |

# COMMAND ----------

# DBTITLE 0,--i18n-1e0888f3-4334-4431-a486-c58f6560210f
# MAGIC %md
# MAGIC
# MAGIC 例えば、各メールアドレスで注文されたマットレスのサイズを確認したい場合、 **`collect_set`** 関数を使用できます。

# COMMAND ----------

size_df = mattress_df.groupBy("email").agg(collect_set("size").alias("size options"))

display(size_df)

# COMMAND ----------

# DBTITLE 0,--i18n-7304a528-9b97-4806-954f-56cbf7bed6dc
# MAGIC %md
# MAGIC
# MAGIC ## Union と unionByName
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> DataFrameの<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.union.html" target="_blank">**`union`**</a> メソッドは、通常のSQLのように列を位置で解決します。 これを使用するのは、2つのDataFrameがスキーマ（列の順序を含む）が完全に同じ場合に限られます。 対照的に、DataFrameの<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.unionByName.html" target="_blank">**`unionByName`**</a> メソッドは列を名前で解決します。 これはSQLのUNION ALLに相当します。 どちらも重複を削除しません。
# MAGIC
# MAGIC 以下は、2つのデータフレームが適切なスキーマを持つかどうかを確認するためのチェックです。 **`union`** が適切な場合です。

# COMMAND ----------

mattress_df.schema==size_df.schema

# COMMAND ----------

# DBTITLE 0,--i18n-7bb80944-614a-487f-85b8-bb3983e259ed
# MAGIC %md
# MAGIC
# MAGIC 2つのスキーマが単純な **`select`** ステートメントで一致する場合、**`union`** を使用できます。

# COMMAND ----------

union_count = mattress_df.select("email").union(size_df.select("email")).count()

mattress_count = mattress_df.count()
size_count = size_df.count()

mattress_count + size_count == union_count

# COMMAND ----------

# DBTITLE 0,--i18n-52fdd386-f0dc-4850-87c7-4775fd2c64d4
# MAGIC %md
# MAGIC
# MAGIC ## Clean up classroom
# MAGIC
# MAGIC 最後に、教室を片付けます。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
