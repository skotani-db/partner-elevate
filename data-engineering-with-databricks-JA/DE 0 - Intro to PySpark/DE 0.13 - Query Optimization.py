# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-15802400-50d0-40e5-854c-89b08b50c14e
# MAGIC %md
# MAGIC
# MAGIC # クエリの最適化
# MAGIC
# MAGIC 論理的な最適化、プレディケートプッシュダウンの例など、いくつかの例におけるクエリプランと最適化を探求します。
# MAGIC
# MAGIC ##### 目標
# MAGIC 1. 論理的な最適化
# MAGIC 1. プレディケートプッシュダウン
# MAGIC 1. プレディケートプッシュダウンなし
# MAGIC
# MAGIC ##### メソッド
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.explain.html#pyspark.sql.DataFrame.explain" target="_blank">DataFrame</a>: **`explain`**

# COMMAND ----------

# DBTITLE 0,--i18n-8cb4efc1-cf1b-42a5-9cf3-109ccc0b5bb5
# MAGIC %md
# MAGIC
# MAGIC セットアップセルを実行し、変数 **`df`** に格納された初期のDataFrameを取得しましょう。このDataFrameを表示することで、イベントデータを確認できます。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.13

# COMMAND ----------

df = spark.read.table("events")
display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-63293e50-d68e-468d-a3c2-08608c66fb1d
# MAGIC %md
# MAGIC
# MAGIC ### 論理的な最適化
# MAGIC
# MAGIC **`explain(..)`** はクエリプランを表示し、オプションで指定された説明モードによってフォーマットされます。以下の論理プランと物理プランを比較し、Catalystが複数の **`filter`** 変換を処理する方法に注意してください。

# COMMAND ----------

from pyspark.sql.functions import col

limit_events_df = (df
                   .filter(col("event_name") != "reviews")
                   .filter(col("event_name") != "checkout")
                   .filter(col("event_name") != "register")
                   .filter(col("event_name") != "email_coupon")
                   .filter(col("event_name") != "cc_info")
                   .filter(col("event_name") != "delivery")
                   .filter(col("event_name") != "shipping_info")
                   .filter(col("event_name") != "press")
                  )

limit_events_df.explain(True)

# COMMAND ----------

# DBTITLE 0,--i18n-cc9b8d61-bb89-4961-819d-d135ec4f4aac
# MAGIC %md
# MAGIC 当然ながら、最初から単一の **`filter`** 条件を使用してクエリを書くことができました。前のクエリプランと次のクエリプランを比較してください。

# COMMAND ----------

better_df = (df
             .filter((col("event_name").isNotNull()) &
                     (col("event_name") != "reviews") &
                     (col("event_name") != "checkout") &
                     (col("event_name") != "register") &
                     (col("event_name") != "email_coupon") &
                     (col("event_name") != "cc_info") &
                     (col("event_name") != "delivery") &
                     (col("event_name") != "shipping_info") &
                     (col("event_name") != "press"))
            )

better_df.explain(True)

# COMMAND ----------

# DBTITLE 0,--i18n-27a81fc2-4aec-46bf-89c0-bb8b90fa9e17
# MAGIC %md
# MAGIC もちろん、意図的に以下のコードを書くことはありませんが、長い複雑なクエリでは重複するフィルタ条件に気付かないかもしれません。このクエリに対してCatalystがどのように振る舞うかを見てみましょう。

# COMMAND ----------

stupid_df = (df
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
            )

stupid_df.explain(True)

# COMMAND ----------

# DBTITLE 0,--i18n-90d320e9-9295-4869-8042-217652fe355b
# MAGIC %md
# MAGIC ### キャッシュ
# MAGIC
# MAGIC デフォルトでは、DataFrameのデータはクエリの実行中にのみSparkクラスタに存在し、その後自動的にクラスタには保存されません（Sparkはデータ処理エンジンであり、データストレージシステムではありません）。DataFrameをクラスタにキャッシュするようにSparkに明示的に要求するには、その **`cache`** メソッドを呼び出します。
# MAGIC
# MAGIC DataFrameをキャッシュする場合、それをもう必要としなくなったら、必ず **`unpersist`** を呼び出してキャッシュから削除するべきです。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_best_32.png" alt="ベストプラクティス"> DataFrameをキャッシュするのは、同じDataFrameを複数回使用することが確実な場合に適しています。たとえば、以下のような場合です。
# MAGIC
# MAGIC - 探索的データ分析
# MAGIC - 機械学習モデルのトレーニング
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="警告"> これらのユースケース以外では、DataFrameをキャッシュすべきではありません。なぜなら、アプリケーションのパフォーマンスが低下する可能性があるからです。
# MAGIC
# MAGIC - キャッシュはタスクの実行に使用できるはずのクラスタリソースを消費します。
# MAGIC - キャッシュは、次の例で示すように、Sparkがクエリの最適化を実行しなくなる可能性があります。

# COMMAND ----------

# DBTITLE 0,--i18n-2256e20c-d69c-4ce8-ae74-c513a8d673f5
# MAGIC %md
# MAGIC
# MAGIC ### プレディケート プッシュダウン
# MAGIC
# MAGIC これはJDBCソースから読み取る例です。ここで、Catalystは*プレディケート プッシュダウン*を実行できることを判断します。

# COMMAND ----------

# MAGIC %scala
# MAGIC // Ensure that the driver class is loaded
# MAGIC Class.forName("org.postgresql.Driver")

# COMMAND ----------

jdbc_url = "jdbc:postgresql://server1.training.databricks.com/training"

# Username and Password w/read-only rights
conn_properties = {
    "user" : "training",
    "password" : "training"
}

pp_df = (spark
         .read
         .jdbc(url=jdbc_url,                 # the JDBC URL
               table="training.people_1m",   # the name of the table
               column="id",                  # the name of a column of an integral type that will be used for partitioning
               lowerBound=1,                 # the minimum value of columnName used to decide partition stride
               upperBound=1000000,           # the maximum value of columnName used to decide partition stride
               numPartitions=8,              # the number of partitions/connections
               properties=conn_properties    # the connection properties
              )
         .filter(col("gender") == "M")   # Filter the data by gender
        )

pp_df.explain(True)

# COMMAND ----------

# DBTITLE 0,--i18n-b067b782-e86b-4284-80f4-4faedfb0953e
# MAGIC %md
# MAGIC
# MAGIC **Filter**の欠如と**Scan**内の**PushedFilters**の存在に注意してください。フィルター操作はデータベースにプッシュされ、一致するレコードのみがSparkに送信されます。これにより、Sparkがインジェストする必要があるデータの量が大幅に削減される可能性があります。あ

# COMMAND ----------

# DBTITLE 0,--i18n-e378204a-cce7-4903-a1e4-f2f3e387c4f5
# MAGIC %md
# MAGIC
# MAGIC ### プレディケート プッシュダウンなし
# MAGIC
# MAGIC 比較として、フィルタリング前にデータをキャッシュすると、プレディケート プッシュダウンの可能性がなくなります。

# COMMAND ----------

cached_df = (spark
            .read
            .jdbc(url=jdbc_url,
                  table="training.people_1m",
                  column="id",
                  lowerBound=1,
                  upperBound=1000000,
                  numPartitions=8,
                  properties=conn_properties
                 )
            )

cached_df.cache()
filtered_df = cached_df.filter(col("gender") == "M")

filtered_df.explain(True)

# COMMAND ----------

# DBTITLE 0,--i18n-7923a69e-43bd-4a4d-8de9-ac83d6eee749
# MAGIC %md
# MAGIC
# MAGIC 前の例で見た**Scan**（JDBCの読み取り）に加えて、ここでは説明計画に**InMemoryTableScan**と**Filter**が表示されます。
# MAGIC
# MAGIC これは、Sparkがデータベースからすべてのデータを読み取り、キャッシュに格納し、その後、キャッシュ内のデータをスキャンしてフィルタ条件に一致するレコードを検索する必要があることを意味します。

# COMMAND ----------

# DBTITLE 0,--i18n-20c1b03f-3627-40bf-b426-f24cb3111430
# MAGIC %md
# MAGIC
# MAGIC 自分たちの後始末を忘れないようにしましょう！

# COMMAND ----------

cached_df.unpersist()

# COMMAND ----------

# DBTITLE 0,--i18n-be8bb4b0-cdcc-4457-baa3-145a71d04b35
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
