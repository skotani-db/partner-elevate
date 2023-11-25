# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # ストリームと静的結合
# MAGIC
# MAGIC このレッスンでは、ストリーミング心拍数データと完成した workouts テーブルを結合します。
# MAGIC
# MAGIC アーキテクチャー図で、テーブル **`workout_bpm`** を作成します。
# MAGIC
# MAGIC このパターンでは、テーブルがクエリされるたびに最新バージョンが返されることを保証する Delta Lake の機能を利用します。
# MAGIC
# MAGIC
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_workout_bpm.png" width="60%" />
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、受講者は以下を習得する：
# MAGIC - ストリーム静的結合のバージョニングとマッチングに関する保証の説明
# MAGIC - Spark SQL と PySpark を活用してストリーム静的結合を処理する

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC **NOTE**: セットアップスクリプトには、以下の結合に必要な **`user_lookup`** テーブルを定義するロジックが含まれています。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-4.5

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming Temporary Viewを設定します。テーブルの **1つ** からしかストリーミングしないことに注意してください。 **`completed_workouts`** テーブルは、Structured Streamingの継続的なソースの要件を満たすことができないため、もはやストリーミングできません。しかし、デルタテーブルとストリーム静的結合を実行する場合、各バッチは静的デルタテーブルの最新バージョンが使用されていることを確認します。

# COMMAND ----------

(spark.readStream
      .table("heart_rate_silver")
      .createOrReplaceTempView("TEMP_heart_rate_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ワークアウトと心拍数の記録を一致させるためのStream-Static Joinの実行
# MAGIC
# MAGIC 以下では、ストリームと **`completed_workouts`** テーブルを結合するクエリを設定します。
# MAGIC
# MAGIC 心拍数の記録には **`device_id`** しかありませんが、ワークアウトには一意の識別子として **`user_id`** が使用されます。 **`user_lookup`** テーブルを使用して、これらの値を一致させる必要があります。全てのテーブルが Delta Lake テーブルであるため、各マイクロバッチトランザクション中に各テーブルの最新バージョンを取得することが保証されている。
# MAGIC
# MAGIC 重要なことは、我々のデバイスが時折、負の記録でメッセージを送信することである。これは、記録された値に潜在的なエラーがあることを意味する。ポジティブな記録のみが処理されるように、述語条件を定義する必要がある。

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMP VIEW TEMP_workout_bpm AS
# MAGIC   SELECT d.user_id, d.workout_id, d.session_id, time, heartrate
# MAGIC   FROM TEMP_heart_rate_silver c
# MAGIC   INNER JOIN (
# MAGIC     SELECT a.user_id, b.device_id, workout_id, session_id, start_time, end_time
# MAGIC     FROM completed_workouts a
# MAGIC     INNER JOIN user_lookup b
# MAGIC     ON a.user_id = b.user_id) d
# MAGIC   ON c.device_id = d.device_id AND time BETWEEN start_time AND end_time
# MAGIC   WHERE c.bpm_check = 'OK'

# COMMAND ----------

# MAGIC %md
# MAGIC 結合のストリーミング部分がこの結合処理を駆動することに注意してください。現在の実装では、このクエリを処理する前に、一致するレコードが **`completed_workouts`** テーブルに書き込まれた場合のみ、 **`heart_rate_silver`** テーブルのレコードが結果テーブルに表示されます。
# MAGIC
# MAGIC つまり、結果を計算する前に、結合の右側にレコードが現れるのを待つようにクエリを構成することはできません。ストリーム静的結合を使用する場合、一致しないレコードに対する潜在的な制限に注意してください。(インクリメンタル実行中に見逃したレコードを検索して挿入するために、別のバッチジョブを構成することができることに注意してください）。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 追記モードでのストリームの書き込み
# MAGIC
# MAGIC 以下では、上記のStreaming Temporary Viewを使用して、新しい値を **`workout_bpm`** テーブルに挿入します。

# COMMAND ----------

def process_workout_bpm():
    query = (spark.table("TEMP_workout_bpm")
                  .writeStream
                  .format("delta")
                  .outputMode("append")
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/workout_bpm")
                  .trigger(availableNow=True)
                  .table("workout_bpm"))
    
    query.awaitTermination()
    
process_workout_bpm()

# COMMAND ----------

# MAGIC %md
# MAGIC この結果表は以下の通り。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM workout_bpm

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workout_bpm

# COMMAND ----------

# MAGIC %md
# MAGIC 必要であれば、全テーブルを通して別のバッチを処理し、これらの結果を更新する。

# COMMAND ----------

DA.daily_stream.load()          # Load one new day for DA.paths.source_daily
DA.process_bronze()             # Process through the bronze table
DA.process_heart_rate_silver()  # Process the heart_rate_silver table
DA.process_workouts_silver()    # Process the workouts_silver table
DA.process_completed_workouts() # Process the completed_workouts table

process_workout_bpm()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM workout_bpm

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
