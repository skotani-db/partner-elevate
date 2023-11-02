# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-df421470-173e-44c6-a85a-1d48d8a14d42
# MAGIC %md
# MAGIC # その他の関数
# MAGIC
# MAGIC ##### 目標
# MAGIC 1. 組み込み関数を使用して新しい列のデータを生成する
# MAGIC 1. DataFrame NA 関数を使用して null 値を処理する
# MAGIC 1. DataFrame を結合する
# MAGIC
# MAGIC ##### メソッド
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join" target="_blank">DataFrame メソッド</a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>: **`fill`**, **`drop`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">組み込み関数</a>:
# MAGIC   - 集計: **`collect_set`**
# MAGIC   - コレクション: **`explode`**
# MAGIC   - 非集計およびその他: **`col`**, **`lit`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.11

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-c80fc2ec-34e6-459f-b5eb-afa660db9491
# MAGIC %md
# MAGIC
# MAGIC ### 集計しない関数とその他の関数
# MAGIC 以下は、いくつかの追加の集計しない関数およびその他の組み込み関数です。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | col / column | 指定した列名に基づいて列を返す |
# MAGIC | lit | リテラル値の列を作成する |
# MAGIC | isnull | 列が null の場合に true を返す |
# MAGIC | rand | [0.0, 1.0) で一様に分布した独立および同一の分布 (i.i.d.) サンプルを持つランダム列を生成する |

# COMMAND ----------

# DBTITLE 0,--i18n-bae51fa6-6275-46ec-854d-40ba81788bac
# MAGIC %md
# MAGIC
# MAGIC **`col`** 関数を使用して特定の列を選択できます。

# COMMAND ----------

gmail_accounts = sales_df.filter(col("email").endswith("gmail.com"))

display(gmail_accounts)

# COMMAND ----------

# DBTITLE 0,--i18n-a88d37a6-5e98-40ad-9045-bb8fd4d36331
# MAGIC %md
# MAGIC
# MAGIC **`lit`** は値から列を作成するために使用でき、列を追加する際に便利です。

# COMMAND ----------

display(gmail_accounts.select("email", lit(True).alias("gmail user")))

# COMMAND ----------

# DBTITLE 0,--i18n-d7436832-7254-4bfa-845b-2ab10170f171
# MAGIC %md
# MAGIC
# MAGIC ### DataFrameNaFunctions
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>は、null値を操作するためのメソッドを提供するDataFrameのサブモジュールです。DataFrameNaFunctionsのインスタンスを取得するには、DataFrameの **`na`** 属性にアクセスします。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | drop | 任意の、すべての、または指定した数のnull値を持つ行を除外した新しいDataFrameを返します。オプションで列のサブセットを考慮します。 |
# MAGIC | fill | null値を指定した値で置き換え、オプションの列のサブセットに適用します。 |
# MAGIC | replace | 指定した値を別の値で置き換え、オプションの列のサブセットに適用した新しいDataFrameを返します。 |

# COMMAND ----------

# DBTITLE 0,--i18n-da0ccd36-e2b5-4f79-ae61-cb8252e5da7c
# MAGIC %md
# MAGIC ここでは、nullまたはNA値を持つ行を削除する前と後の行数を確認します。

# COMMAND ----------

print(sales_df.count())
print(sales_df.na.drop().count())

# COMMAND ----------

# DBTITLE 0,--i18n-aef560b8-7bb6-4985-a43d-38541ba78d33
# MAGIC %md
# MAGIC 行数が同じであるため、nullの列はありません。`items`を展開して、`items.coupon`などの列にいくつかのnullを見つける必要があります。

# COMMAND ----------

sales_exploded_df = sales_df.withColumn("items", explode(col("items")))
display(sales_exploded_df.select("items.coupon"))
print(sales_exploded_df.select("items.coupon").count())
print(sales_exploded_df.select("items.coupon").na.drop().count())

# COMMAND ----------

# DBTITLE 0,--i18n-4c01038b-2afa-41d8-a390-fad45d1facfe
# MAGIC %md
# MAGIC
# MAGIC 欠落しているクーポンコードは、 **`na.fill`** を使用して補完できます。

# COMMAND ----------

display(sales_exploded_df.select("items.coupon").na.fill("NO COUPON"))

# COMMAND ----------

# DBTITLE 0,--i18n-8a65ceb6-7bc2-4147-be66-71b63ae374a1
# MAGIC %md
# MAGIC
# MAGIC ### データフレームの結合
# MAGIC
# MAGIC DataFrameの<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join" target="_blank">**`join`**</a>メソッドは、指定された結合式に基づいて2つのDataFrameを結合します。
# MAGIC
# MAGIC さまざまな種類の結合がサポートされています：
# MAGIC
# MAGIC "名前"という共有列の等しい値に基づく内部結合（equi join）<br/>
# MAGIC **`df1.join(df2, "name")`**
# MAGIC
# MAGIC "名前"と"age"という共有列の等しい値に基づく内部結合<br/>
# MAGIC **`df1.join(df2, ["name", "age"])`**
# MAGIC
# MAGIC "名前"という共有列の等しい値に基づくフルアウター結合<br/>
# MAGIC **`df1.join(df2, "name", "outer")`**
# MAGIC
# MAGIC 明示的な列式に基づく左外部結合<br/>
# MAGIC **`df1.join(df2, df1["customer_name"] == df2["account_name"], "left_outer")`**

# COMMAND ----------

# DBTITLE 0,--i18n-67001b92-91e6-4137-9138-b7f00950b450
# MAGIC %md
# MAGIC 上記で作成したgmail_accountsのデータと結合するために、ユーザーデータを読み込みます。

# COMMAND ----------

users_df = spark.table("users")
display(users_df)

# COMMAND ----------

joined_df = gmail_accounts.join(other=users_df, on='email', how = "inner")
display(joined_df)

# COMMAND ----------

# DBTITLE 0,--i18n-9a47f04c-ce9c-4892-b457-1a2e57176721
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
