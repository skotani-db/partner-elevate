# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # ストリーミング重複排除
# MAGIC
# MAGIC このノートブックでは、Structured Streaming と Delta Lake を使って重複レコードを削除する方法を学びます。Spark Structured Streaming は正確に一度だけの処理を保証しますが、多くのソースシステムでは重複レコードが発生します。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、以下のことができるようになります：
# MAGIC -  **`dropDuplicates`** をストリーミングデータに適用する
# MAGIC - Watermarkを使用して状態情報を管理する
# MAGIC - デルタテーブルに重複レコードを挿入しないように、挿入のみのマージを記述する
# MAGIC -  **`foreachBatch`** を使用して、ストリーミング・アップサートを実行する。

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ
# MAGIC データベースを宣言し、すべてのパス変数を設定します。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-4.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 重複レコードの特定
# MAGIC
# MAGIC Kafkaはデータ配信においてat-least-once保証を提供するため、すべてのKafkaコンシューマーは重複レコードを処理する準備をする必要があります。
# MAGIC
# MAGIC ここで紹介する重複排除の方法は、Delta Lake アプリケーションの他の部分でも必要に応じて適用することができます。
# MAGIC
# MAGIC まず、ブロンズテーブルの **`bpm`** トピックで重複レコードの数を特定することから始めましょう。

# COMMAND ----------

total = (spark.read
              .table("bronze")
              .filter("topic = 'bpm'")
              .count())

print(f"Total: {total:,}")

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

old_total = (spark.read
                  .table("bronze")
                  .filter("topic = 'bpm'")
                  .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                  .select("v.*")
                  .dropDuplicates(["device_id", "time"])
                  .count())

print(f"Old Total: {old_total:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC レコードの約10-20％が重複しているようだ。ここでは、ブロンズレベルではなくシルバーレベルで重複排除を適用することにしている。重複レコードを保存している一方で、ブロンズ・テーブルはストリーミング・ソースの真の状態 の履歴を保持し、すべてのレコードを到着したときの状態で表示する（いくつかの追加メタデータが記録されている）。これにより、必要であればダウンストリーム・システムのあらゆる状態を再作成することができ、最初の取り込み時に過度に積極的な品質管理が行われることによる潜在的なデータ損失を防ぐだけでなく、データ取り込みの待ち時間を最小限に抑えることができます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ブロンズBPMレコードのストリーミングリードを定義する
# MAGIC
# MAGIC ここで、前回のノートにあった最後のロジックを復活させる。

# COMMAND ----------

json_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

bpm_df = (spark.readStream
               .table("bronze")
               .filter("topic = 'bpm'")
               .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
               .select("v.*"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ストリーミング重複排除を扱う場合、静的データと比較して複雑なレベルがある。
# MAGIC
# MAGIC 各マイクロバッチが処理されるとき、以下のことを確認する必要がある：
# MAGIC - マイクロバッチに重複レコードが存在しない。
# MAGIC - 挿入されるレコードがターゲットテーブルに既に存在しない
# MAGIC
# MAGIC Spark Structured Streamingは、マイクロバッチ内やマイクロバッチ間で重複レコードが存在しないように、uniqueキーの状態情報を追跡できます。時間の経過とともに、この状態情報はすべての履歴を表すようにスケールする。適切な期間のWatermarkを適用することで、レコードが遅延する可能性があると合理的に予想される時間のウィンドウの状態情報だけを追跡することができる。ここでは、このWatermarkを30秒と定義する。
# MAGIC
# MAGIC 以下のセルは、前回のクエリを更新したものである。

# COMMAND ----------

json_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'bpm'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("time", "30 seconds")
                   .dropDuplicates(["device_id", "time"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 挿入のみのマージ
# MAGIC Delta Lake は、挿入のみのマージに最適化された機能を持っています。この操作は重複排除に理想的です。一意のキーでマッチするロジックを定義し、まだ存在しないキーのレコードだけを挿入します。
# MAGIC
# MAGIC このアプリケーションでは、同じマッチングキーを持つ2つのレコードが同じ情報を表していることが分かっているため、この方法で処理を進めることに注意されたい。後に到着したレコードが既存のレコードに必要な変更を示していた場合、 **`WHEN MATCHED`** 句を含むようにロジックを変更する必要があります。
# MAGIC
# MAGIC 以下のSQLでは、 **`stream_updates`** というタイトルのビューに対するマージクエリを定義しています。

# COMMAND ----------

sql_query = """
  MERGE INTO heart_rate_silver a
  USING stream_updates b
  ON a.device_id=b.device_id AND a.time=b.time
  WHEN NOT MATCHED THEN INSERT *
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## **`foreachBatch`** 用マイクロバッチ関数の定義
# MAGIC
# MAGIC Spark Structured Streaming の **`foreachBatch`** メソッドでは、書き込み時にカスタムロジックを定義できます。
# MAGIC
# MAGIC  **`foreachBatch`** で適用されるロジックは、現在のマイクロバッチを（ストリーミングではなく）バッチデータであるかのように扱います。
# MAGIC
# MAGIC 以下のセルで定義されているクラスは、構造化ストリーミング書き込みで使用するために、任意の SQL **`MERGE INTO`** クエリを登録できるようにする単純なロジックを定義しています。

# COMMAND ----------

class Upsert:
    def __init__(self, sql_query, update_temp="stream_updates"):
        self.sql_query = sql_query
        self.update_temp = update_temp 
        
    def upsert_to_delta(self, microBatchDF, batch):
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC SQLを使ってデルタ・テーブルに書き込むので、始める前にこのテーブルが存在することを確認する必要がある。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS heart_rate_silver 
# MAGIC (device_id LONG, time TIMESTAMP, heartrate DOUBLE)
# MAGIC USING DELTA
# MAGIC LOCATION '${da.paths.user_db}/heart_rate_silver'

# COMMAND ----------

# MAGIC %md
# MAGIC ここで、先に定義した **`sql_query`** を **`Upsert`** クラスに渡します。

# COMMAND ----------

streaming_merge = Upsert(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC そして、このクラスを **`foreachBatch`** ロジックで使用する。

# COMMAND ----------

query = (deduped_df.writeStream
                   .foreachBatch(streaming_merge.upsert_to_delta)
                   .outputMode("update")
                   .option("checkpointLocation", f"{DA.paths.checkpoints}/recordings")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC **`heart_rate_silver`** テーブルに処理された一意なエントリの数が、上記のバッチ重複排除クエリと一致していることがわかる。

# COMMAND ----------

new_total = spark.read.table("heart_rate_silver").count()

print(f"Old Total: {old_total:,}")
print(f"New Total: {new_total:,}")

# COMMAND ----------

# MAGIC %md 
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
