# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-3fbfc7bd-6ef2-4fea-b8a2-7f949cd84044
# MAGIC %md
# MAGIC # 集計
# MAGIC
# MAGIC ##### 目的
# MAGIC 1. 指定された列でデータをグループ化する
# MAGIC 1. データを集計するためにグループ化されたデータメソッドを適用する
# MAGIC 1. データを集計するために組み込み関数を適用する
# MAGIC
# MAGIC ##### メソッド
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank" target="_blank">グループ化されたデータ</a>: **`agg`**, **`avg`**, **`count`**, **`max`**, **`sum`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">組み込み関数</a>: **`approx_count_distinct`**, **`avg`**, **`sum`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.05

# COMMAND ----------

# DBTITLE 0,--i18n-88095892-40a1-46dd-a809-19186953d968
# MAGIC %md
# MAGIC
# MAGIC BedBricksイベントデータセットを使用しましょう。

# COMMAND ----------

df = spark.table("events")
display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-a04aa8bd-35f0-43df-b137-6e34aebcded1
# MAGIC %md
# MAGIC
# MAGIC ### Grouping data
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/aspwd/aggregation_groupby.png" width="60%" />

# COMMAND ----------

# DBTITLE 0,--i18n-cd0936f7-cd8a-4277-bbaf-d3a6ca2c29ec
# MAGIC %md
# MAGIC
# MAGIC ### groupBy
# MAGIC Use the DataFrame **`groupBy`** method to create a grouped data object. 
# MAGIC
# MAGIC This grouped data object is called **`RelationalGroupedDataset`** in Scala and **`GroupedData`** in Python.

# COMMAND ----------

df.groupBy("event_name")

# COMMAND ----------

df.groupBy("geo.state", "geo.city")

# COMMAND ----------

# DBTITLE 0,--i18n-7918f032-d001-4e38-bd75-51eb68c41ffa
# MAGIC %md
# MAGIC
# MAGIC ### グループ化されたデータのメソッド
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank">グループ化されたデータ</a> オブジェクトにはさまざまな集計メソッドがあります。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | agg | 一連の集計列を指定して集計を計算します |
# MAGIC | avg | 各グループの各数値列の平均値を計算します |
# MAGIC | count | 各グループの行数をカウントします |
# MAGIC | max | 各グループの各数値列の最大値を計算します |
# MAGIC | mean | 各グループの各数値列の平均値を計算します |
# MAGIC | min | 各グループの各数値列の最小値を計算します |
# MAGIC | pivot | 現在のDataFrameの列をピボットし、指定された集計を実行します |
# MAGIC | sum | 各グループの各数値列の合計を計算します |

# COMMAND ----------

event_counts_df = df.groupBy("event_name").count()
display(event_counts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-bf63efea-c4f7-4ff9-9d42-4de245617d97
# MAGIC %md
# MAGIC
# MAGIC ここでは、各グループごとの平均購入収益を取得しています。

# COMMAND ----------

avg_state_purchases_df = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avg_state_purchases_df)

# COMMAND ----------

# DBTITLE 0,--i18n-b11167f4-c270-4f7b-b967-75538237c915
# MAGIC %md
# MAGIC また、州と都市の組み合わせごとに、数量と購入収益の合計を取得しています。

# COMMAND ----------

city_purchase_quantities_df = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity", "ecommerce.purchase_revenue_in_usd")
display(city_purchase_quantities_df)

# COMMAND ----------

# DBTITLE 0,--i18n-62a4e852-249a-4dbf-b47a-e85a64cbc258
# MAGIC %md
# MAGIC
# MAGIC DataFrameやColumnの変換メソッドに加えて、Sparkの組み込み<a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html" target="_blank">SQL関数</a>モジュールには非常に役立つ関数がたくさんあります。
# MAGIC
# MAGIC Scalaでは、これは<a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">**`org.apache.spark.sql.functions`**</a>であり、Pythonでは<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions" target="_blank">**`pyspark.sql.functions`**</a>です。このモジュールの関数は、コードにインポートする必要があります。

# COMMAND ----------

# DBTITLE 0,--i18n-68f06736-e457-4893-8c0d-be83c818bd91
# MAGIC %md
# MAGIC
# MAGIC ### 集計関数
# MAGIC
# MAGIC 以下は、集計に使用できる組み込み関数の一部です。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | グループ内の一意のアイテムの近似数を返します |
# MAGIC | avg | グループ内の値の平均値を返します |
# MAGIC | collect_list | 重複を含むオブジェクトのリストを返します |
# MAGIC | corr | 2つの列のピアソン相関係数を返します |
# MAGIC | max | 各グループの各数値列の最大値を計算します |
# MAGIC | mean | 各グループの各数値列の平均値を計算します |
# MAGIC | stddev_samp | グループ内の式のサンプル標準偏差を返します |
# MAGIC | sumDistinct | 式内の一意の値の合計を返します |
# MAGIC | var_pop | グループ内の値の母集団分散を返します |
# MAGIC
# MAGIC 組み込みの集計関数を適用するために、グループ化されたデータメソッド<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">**`agg`**</a>を使用します。
# MAGIC
# MAGIC これにより、結果の列に<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">**`alias`**</a>などの他の変換を適用できます。

# COMMAND ----------

from pyspark.sql.functions import sum

state_purchases_df = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(state_purchases_df)

# COMMAND ----------

# DBTITLE 0,--i18n-875e6ef8-fec3-4468-ab86-f3f6946b281f
# MAGIC %md
# MAGIC
# MAGIC グループ化されたデータに複数の集計関数を適用します。

# COMMAND ----------

from pyspark.sql.functions import avg, approx_count_distinct

state_aggregates_df = (df
                       .groupBy("geo.state")
                       .agg(avg("ecommerce.total_item_quantity").alias("avg_quantity"),
                            approx_count_distinct("user_id").alias("distinct_users"))
                      )

display(state_aggregates_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6bb4a15f-4f5d-4f70-bf50-4be00167d9fa
# MAGIC %md
# MAGIC
# MAGIC ### 数学関数
# MAGIC 以下は、数学演算に使用できる組み込み関数のいくつかです。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | ceil | 指定された列の天井値を計算します。 |
# MAGIC | cos | 指定された値の余弦を計算します。 |
# MAGIC | log | 指定された値の自然対数を計算します。 |
# MAGIC | round | HALF_UP丸めモードで0桁までの列eの値を返します。 |
# MAGIC | sqrt | 指定された浮動小数点値の平方根を計算します。 |

# COMMAND ----------

from pyspark.sql.functions import cos, sqrt

display(spark.range(10)  # Create a DataFrame with a single column called "id" with a range of integer values
        .withColumn("sqrt", sqrt("id"))
        .withColumn("cos", cos("id"))
       )

# COMMAND ----------

# DBTITLE 0,--i18n-d03fb77f-5e4c-43b8-a293-884cd7cb174c
# MAGIC %md
# MAGIC
# MAGIC このレッスンに関連するテーブルやファイルを削除するには、以下のセルを実行してください。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
