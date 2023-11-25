# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # バッチジョブのスケジューリング
# MAGIC
# MAGIC このノートブックはバッチジョブとしてスケジュールすることを想定しています。
# MAGIC
# MAGIC ノートブックをスケジュールする前に、以下の事項についてコメントアウトすることをお勧めします:
# MAGIC - 開発中に追加されたファイルの削除コマンド
# MAGIC - データベースやテーブルのドロップや作成のコマンド（実行ごとにこれらを新たに作成する必要がない場合）
# MAGIC - ノートブックに結果を具現化する任意のアクション/SQLクエリ（人間が定期的にこのビジュアル出力を確認する場合を除く）
# MAGIC
# MAGIC ### インタラクティブクラスタに対するスケジューリング
# MAGIC
# MAGIC データが小さく、ここで実行するクエリが比較的速く完了するため、既にオンになっているコンピュートを利用してこのノートブックをスケジュールします。
# MAGIC
# MAGIC このジョブを手動でトリガーするか、1分ごとに更新するようにスケジュールすることができます。
# MAGIC
# MAGIC 必要であれば、 **`Run Now`** をクリックするか、次の分の先頭まで待って自動的にトリガーされるのを待ちます。
# MAGIC
# MAGIC ### ベストプラクティス：ウォームプール
# MAGIC
# MAGIC このデモでは、既にオンになっているコンピュートを利用してコードを実行する際の摩擦と複雑さを減らすため、意図的にこの方法を選択しています。本番環境では、このような短い実行時間で頻繁にトリガーされるジョブは、<a href="https://docs.microsoft.com/ja-jp/azure/databricks/clusters/instance-pools/" target="_blank">ウォームプール</a>に対してスケジュールすることが推奨されます。
# MAGIC
# MAGIC プールを使用すると、アイドル状態のコンピュートに対するDBUの請求を削除しながら、スケジュールされたジョブに対して準備が整ったコンピュートリソースを柔軟に利用できます。請求されるDBUは、すべての用途のワークロードではなく、ジョブに対してのみ請求されます（これはより低コストです）。また、インタラクティブクラスタではなくプールを使用することで、単一のクラスタを共有するジョブ間やスケジュールされたジョブとインタラクティブクエリ間のリソース競合の可能性を排除します。

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.4.3

# COMMAND ----------

from pyspark.sql import functions as F

# user_bins
def age_bins(dob_col):
    age_col = F.floor(F.months_between(F.current_date(), dob_col)/12).alias("age")
    return (F.when((age_col < 18), "under 18")
            .when((age_col >= 18) & (age_col < 25), "18-25")
            .when((age_col >= 25) & (age_col < 35), "25-35")
            .when((age_col >= 35) & (age_col < 45), "35-45")
            .when((age_col >= 45) & (age_col < 55), "45-55")
            .when((age_col >= 55) & (age_col < 65), "55-65")
            .when((age_col >= 65) & (age_col < 75), "65-75")
            .when((age_col >= 75) & (age_col < 85), "75-85")
            .when((age_col >= 85) & (age_col < 95), "85-95")
            .when((age_col >= 95), "95+")
            .otherwise("invalid age").alias("age"))

lookupDF = spark.table("user_lookup").select("alt_id", "user_id")
binsDF = spark.table("users").join(lookupDF, ["alt_id"], "left").select("user_id", age_bins(F.col("dob")),"gender", "city", "state")

(binsDF.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable("user_bins"))

# COMMAND ----------

# completed_workouts
spark.sql("""
    CREATE OR REPLACE TEMP VIEW TEMP_completed_workouts AS (
      SELECT a.user_id, a.workout_id, a.session_id, a.start_time start_time, b.end_time end_time, a.in_progress AND (b.in_progress IS NULL) in_progress
      FROM (
        SELECT user_id, workout_id, session_id, time start_time, null end_time, true in_progress
        FROM workouts_silver
        WHERE action = "start") a
      LEFT JOIN (
        SELECT user_id, workout_id, session_id, null start_time, time end_time, false in_progress
        FROM workouts_silver
        WHERE action = "stop") b
      ON a.user_id = b.user_id AND a.session_id = b.session_id
    )
""")

(spark.table("TEMP_completed_workouts").write
      .mode("overwrite")
      .saveAsTable("completed_workouts"))

# COMMAND ----------

#workout_bpm
spark.readStream.table("heart_rate_silver").createOrReplaceTempView("TEMP_heart_rate_silver")

spark.sql("""
  SELECT d.user_id, d.workout_id, d.session_id, time, heartrate
  FROM TEMP_heart_rate_silver c
  INNER JOIN (
    SELECT a.user_id, b.device_id, workout_id, session_id, start_time, end_time
    FROM completed_workouts a
    INNER JOIN user_lookup b
    ON a.user_id = b.user_id) d
  ON c.device_id = d.device_id AND time BETWEEN start_time AND end_time
  WHERE c.bpm_check = 'OK'""").createOrReplaceTempView("TEMP_workout_bpm")

query = (spark.table("TEMP_workout_bpm")
              .writeStream
              .outputMode("append")
              .option("checkpointLocation", f"{DA.paths.checkpoints}/workout_bpm")
              .trigger(availableNow=True)
              .table("workout_bpm"))

query.awaitTermination()

# COMMAND ----------

# workout_bpm_summary
spark.readStream.table("workout_bpm").createOrReplaceTempView("TEMP_workout_bpm")

df = (spark.sql("""
SELECT workout_id, session_id, a.user_id, age, gender, city, state, min_bpm, avg_bpm, max_bpm, num_recordings
FROM user_bins a
INNER JOIN
  (SELECT user_id, workout_id, session_id, MIN(heartrate) min_bpm, MEAN(heartrate) avg_bpm, MAX(heartrate) max_bpm, COUNT(heartrate) num_recordings
  FROM TEMP_workout_bpm
  GROUP BY user_id, workout_id, session_id) b
ON a.user_id = b.user_id"""))

query = (df.writeStream
           .option("checkpointLocation", f"{DA.paths.checkpoints}/workout_bpm_summary")
           .outputMode("complete")
           .trigger(availableNow=True)
           .table("workout_bpm_summary"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 他のレッスンとは異なり、このデモでは **`DA.cleanup()`** コマンドを実行しません。
# MAGIC
# MAGIC なぜなら、このデモのすべてのノートブックでこれらのアセットを維持したいからです。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
