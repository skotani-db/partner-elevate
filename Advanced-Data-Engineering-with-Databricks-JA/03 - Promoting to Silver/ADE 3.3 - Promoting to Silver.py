# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## シルバーへの昇格
# MAGIC
# MAGIC ここでは、デルタ・テーブルのストリーミング、重複排除、品質エンフォースメントのコンセプトをまとめ、シルバー・テーブルへのアプローチを最終決定する。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_heartrate_silver.png" width="60%" />
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、受講者は以下のことができるようになります：
# MAGIC - Delta Lakeテーブルにテーブル制約を適用する
# MAGIC - フラグを使用して、特定の条件を満たさないレコードを特定する
# MAGIC - インクリメンタルマイクロバッチ内で重複排除を適用する
# MAGIC - **`MERGE`** を使用して、Delta Lake テーブルへの重複レコードの挿入を回避する

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-4.3

# COMMAND ----------

# MAGIC %md
# MAGIC  **`heart_rate_silver`** テーブルを作成します。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS heart_rate_silver
# MAGIC   (device_id LONG, time TIMESTAMP, heartrate DOUBLE, bpm_check STRING)
# MAGIC USING DELTA
# MAGIC LOCATION '${da.paths.user_db}/heart_rate_silver'

# COMMAND ----------

# MAGIC %md
# MAGIC ## テーブル制約
# MAGIC データを挿入する前にテーブル制約を追加します。この制約に **`dateWithinRange`** という名前を付け、時間が 2017 年 1 月 1 日より大きいことを確認してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC ALTER TABLE heart_rate_silver ADD CONSTRAINT dateWithinRange CHECK(time > "2017-01-01") -- <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC 制約の追加と削除はトランザクション・ログに記録されることに注意してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY heart_rate_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## ストリーミング読み込みと変換の定義
# MAGIC 以下のセルを使って、以下を含むストリーミング・リードを作成する：
# MAGIC 1. トピック **`bpm`** に対するフィルタ。
# MAGIC 2. JSON ペイロードをフラット化し、データを適切なスキーマにキャストするロジック。
# MAGIC 3. ネガティブレコードにフラグを立てるための **`bpm_check`** カラム
# MAGIC 4. **`device_id`** と **`time`** の重複チェックと、 **`time`** に30秒のWatermarkを入れる。

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

streaming_df = (spark.readStream
                     .table("bronze")
                     .filter("topic = 'bpm'")
                     .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                     .select("v.*", F.when(F.col("v.heartrate") <= 0, "Negative BPM")
                                     .otherwise("OK")
                                     .alias("bpm_check"))
                     .withWatermark("time", "30 seconds")
                     .dropDuplicates(["device_id", "time"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## アップサート・クエリーの定義
# MAGIC 以下に、前のノートブックで使用したupsertクラスを示します。

# COMMAND ----------

class Upsert:
    def __init__(self, sql_query, update_temp="stream_updates"):
        self.sql_query = sql_query
        self.update_temp = update_temp 
        
    def upsert_to_delta(self, micro_batch_df, batch):
        micro_batch_df.createOrReplaceTempView(self.update_temp)
        micro_batch_df._jdf.sparkSession().sql(self.sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のセルを使用して、クラスをインスタンス化するためのアップサート・クエリーを定義します。
# MAGIC
# MAGIC 別の方法として、<a href="https://docs.databricks.com/delta/delta-update.html#upsert-into-a-table-using-merge&language-python" target="_blank">ドキュメント</a>を参照し、 **`DeltaTable`** Python クラスを使って実装してみてください。

# COMMAND ----------

# TODO
sql_query = """
MERGE INTO heart_rate_silver a
USING stream_updates b
ON a.device_id=b.device_id and a.time=b.time
WHEN NOT MATCHED THEN INSERT *
"""
 
streaming_merge=Upsert(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## アップサートと書き込みの適用
# MAGIC ブロンズ・テーブルからすべての既存データを処理するために、trigger-available-now ロジックで書き込みを実行します。

# COMMAND ----------

def process_silver_heartrate():
    query = (streaming_df.writeStream
                         .foreachBatch(streaming_merge.upsert_to_delta)
                         .outputMode("update")
                         .option("checkpointLocation", f"{DA.paths.checkpoints}/recordings")
                         .trigger(availableNow=True)
                         .start())
    query.awaitTermination()
    
process_silver_heartrate()

# COMMAND ----------

# MAGIC %md
# MAGIC シルバーテーブルには、レッスン2.5の重複除外カウントと同じ数の総レコードが表示され、そのうちのごく一部のレコードには"Negative BPM"のフラグが正しく付けられます。

# COMMAND ----------

new_total = spark.read.table("heart_rate_silver").count()

print(f"Lesson #5: {731987:,}")
print(f"New Total: {new_total:,}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM heart_rate_silver
# MAGIC WHERE bpm_check = "Negative BPM"

# COMMAND ----------

# MAGIC %md
# MAGIC 次に、新しいデータのバッチをランディングし、ブロンズを通してシルバーテーブルに変更を伝播する。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> 以下の2つのメソッドは、以前のレッスンから再現したものである。

# COMMAND ----------

DA.daily_stream.load() # Load a day's worth of data
DA.process_bronze()    # Execute 1 iteration of the daily to bronze stream

process_silver_heartrate()

# COMMAND ----------

end_total = spark.read.table("heart_rate_silver").count()

print(f"Lesson #5:   {731987:,}")
print(f"New Total:   {new_total:,}")
print(f"End Total: {end_total:,}")

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
