# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # PIIを安全に保管
# MAGIC
# MAGIC インクリメンタルワークロードに仮名化キーを追加するのは、変換を追加するのと同じくらい簡単です。
# MAGIC
# MAGIC このノートブックでは、PIIを安全に保管し、正確に更新するためのデザインパターンを検討する。また、削除要求が適切にキャプチャされるように処理するためのアプローチも示します。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_users.png" width="60%" />
# MAGIC
# MAGIC ## 学習目標
# MAGIC このノートの終わりには、以下のことができるようになります：
# MAGIC - インクリメンタル変換を適用して、仮名化されたキーでデータを保存する
# MAGIC - ウィンドウランキングを使用して、CDC フィード内の最新のレコードを特定する

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のセルを実行して、関連するデータベースとパスを設定することから始める。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.2

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のセルを実行して、 **`users`** テーブルを作成する。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE users
# MAGIC (alt_id STRING, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, city STRING, state STRING, zip INT, updated TIMESTAMP)
# MAGIC USING DELTA
# MAGIC LOCATION '${da.paths.working_dir}/users'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 仮名化付きELT
# MAGIC **`user_info`** トピックのデータには、Change Data Capture フィードからの完全な行出力が含まれています。
# MAGIC
# MAGIC **`update_type`** には3つの値があります： **`new`** 、 **`update`** 、 **`delete`** である。
# MAGIC
# MAGIC **`users`** テーブルはType 1 のテーブルとして実装されるため、最新の値のみが重要である。
# MAGIC
# MAGIC 以下のセルを実行し、 **`new`** と **`update`** の両方のレコードに **`users`** テーブルに必要なすべてのフィールドが含まれていることを視覚的に確認する。

# COMMAND ----------

from pyspark.sql import functions as F

schema = """
    user_id LONG, 
    update_type STRING, 
    timestamp FLOAT, 
    dob STRING, 
    sex STRING, 
    gender STRING, 
    first_name STRING, 
    last_name STRING, 
    address STRUCT<
        street_address: STRING, 
        city: STRING, 
        state: STRING, 
        zip: INT>"""

users_df = (spark.table("bronze")
                 .filter("topic = 'user_info'")
                 .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
                 .filter(F.col("update_type").isin(["new", "update"])))

display(users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ウィンドウ付きランキングによる重複排除
# MAGIC
# MAGIC 以前、重複レコードを削除する方法をいくつか検討した：
# MAGIC - Delta Lake の **`MERGE`** 構文を使用することで、キーに基づいたレコードの更新や挿入を行うことができ、新しいレコードを以前に読み込まれたデータと照合することができる。
# MAGIC - **`dropDuplicates`** は、テーブルまたはインクリメンタルマイクロバッチ内の正確な重複を削除します。
# MAGIC
# MAGIC ある主キーに対して複数のレコードがあるが、これらのレコードは同一ではない。 **`dropDuplicates`** はこれらのレコードの削除には機能せず、同じキーが複数回存在する場合はマージステートメントでエラーが発生します。
# MAGIC
# MAGIC 以下に、<a href="http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Window.html?highlight=window#pyspark.sql.Window" target="_blank">PySpark Window class</a>を使用した、重複を削除するための3つ目のアプローチを示します。.

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("user_id").orderBy(F.col("timestamp").desc())

ranked_df = (users_df.withColumn("rank", F.rank().over(window))
                     .filter("rank == 1")
                     .drop("rank"))
display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 希望通り、一意な **`user_id`** ごとに、最新の（ **`rank == 1`** ）エントリのみを取得する。
# MAGIC
# MAGIC 残念ながら、これをデータのストリーミング読み取りに適用しようとすると、次のことがわかります。
# MAGIC > Non-time-based windows are not supported on streaming DataFrames
# MAGIC
# MAGIC このエラーを実際に見るには、以下のセルのコメントを外して実行してください：

# COMMAND ----------

# ranked_df = (spark.readStream
#                   .table("bronze")
#                   .filter("topic = 'user_info'")
#                   .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
#                   .select("v.*")
#                   .filter(F.col("update_type").isin(["new", "update"]))
#                   .withColumn("rank", F.rank().over(window))
#                   .filter("rank == 1").drop("rank"))

# display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 幸いなことに、この制限を回避する回避策がある。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ストリーミング・ランク付き重複排除の実装
# MAGIC
# MAGIC 前回見たように、Structured Streaming ジョブに **`MERGE`** ロジックを適用する場合、 **`foreachBatch`** ロジックを使う必要がある。
# MAGIC
# MAGIC ストリーミングマイクロバッチの中にいる間は、バッチ構文を使ってデータを操作することを思い出してください。
# MAGIC
# MAGIC つまり、ランク付けされた **`Window`** ロジックを **`foreachBatch`** 関数内で適用できれば、制限によってエラーがスローされるのを回避できます。
# MAGIC
# MAGIC 以下のコードは、ブロンズテーブルから正しいスキーマでデータをロードするために必要なすべてのインクリメンタルロジックを設定します。これには以下が含まれます：
# MAGIC - **`user_info`** トピックのフィルター
# MAGIC - バッチ内の同一レコードの削除
# MAGIC - **`value`** カラムからすべての JSON フィールドを正しいスキーマにアンパックする。
# MAGIC - フィールド名と型を更新して、 **`users`** テーブルスキーマと一致させる
# MAGIC - Saltedハッシュ関数を使用して、 **`user_id`** を **`alt_id`** にキャストする。

# COMMAND ----------

salt = "BEANS"

unpacked_df = (spark.readStream
                    .table("bronze")
                    .filter("topic = 'user_info'")
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                    .select("v.*")
                    .select(F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("alt_id"),
                            F.col("timestamp").cast("timestamp").alias("updated"),
                            F.to_date("dob", "MM/dd/yyyy").alias("dob"), "sex", "gender", "first_name", "last_name", "address.*", "update_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC 更新されたWindowロジックを以下に示す。このロジックはそれぞれの **`micro_batch_df`** に適用され、マージに使用されるローカルの **`ranked_df`** になることに注意してください。
# MAGIC  
# MAGIC **`MERGE`** ステートメントでは、以下のことが必要です：
# MAGIC - **`alt_id`** のエントリーにマッチする。
# MAGIC - 新しいレコードが前のエントリーより新しい場合、すべてを更新する。
# MAGIC - 一致しない場合、すべてを挿入する
# MAGIC
# MAGIC 前回と同様に、構造化ストリーミングでマージ操作を適用するには **`foreachBatch`** を使用する。

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("alt_id").orderBy(F.col("updated").desc())

def batch_rank_upsert(microBatchDF, batchId):
    
    (microBatchDF.filter(F.col("update_type").isin(["new", "update"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank == 1")
                 .drop("rank")
                 .createOrReplaceTempView("ranked_updates"))
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO users u
        USING ranked_updates r
        ON u.alt_id=r.alt_id
            WHEN MATCHED AND u.updated < r.updated
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC これで、この関数をデータに適用することができる。
# MAGIC
# MAGIC ここでは、全てのレコードを処理するために、trigger-available-nowバッチを実行する。

# COMMAND ----------

query = (unpacked_df.writeStream
                    .foreachBatch(batch_rank_upsert)
                    .outputMode("update")
                    .option("checkpointLocation", f"{DA.paths.checkpoints}/batch_rank_upsert")
                    .trigger(availableNow=True)
                    .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC **`users`** テーブルには、一意なIDごとに1レコードしかないはずである。

# COMMAND ----------

count_a = spark.table("users").count()
count_b = spark.table("users").select("alt_id").distinct().count()
assert count_a == count_b
print("All tests passed.")

# COMMAND ----------

# MAGIC %md 
# MAGIC 以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除します。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
