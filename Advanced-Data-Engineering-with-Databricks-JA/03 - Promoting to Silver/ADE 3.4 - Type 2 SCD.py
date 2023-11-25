# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Type 2 ゆっくりと変化するデータ
# MAGIC
# MAGIC このノートブックでは、ワークアウトと心拍数の記録をリンクさせるために必要な情報を含むシルバーテーブルを作成します。
# MAGIC
# MAGIC このデータを記録するためにType 2のテーブルを使用し、各セッションの開始時刻と終了時刻をエンコードします。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_completed_workouts.png" width="60%" />
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、生徒は以下を習得する：
# MAGIC - Lakehouse で Slowly Changing Dimension テーブルを実装する方法を説明する
# MAGIC - カスタムロジックを使用して、バッチ上書きロジックを持つSCD Type 2テーブルを実装する

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ
# MAGIC パス変数とチェックポイント変数を設定する（これらは後で使用する）。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-4.4

# COMMAND ----------

# MAGIC %md
# MAGIC ## workouts_silverテーブルのレビュー
# MAGIC いくつかのヘルパー関数が、 **`workouts_silver`** テーブルへのデータの一括着陸と伝搬のために定義された。
# MAGIC
# MAGIC このテーブルは以下のようにして作成される。
# MAGIC * テーブルに対してストリームを開始する。
# MAGIC * すべてのレコードを **`topic = 'workout'`** でフィルタリングする。
# MAGIC * データを除外する 
# MAGIC * 一致しないレコードを **`workouts_silver`** にマージする。
# MAGIC
# MAGIC ...先ほど **`heart_rate_silver`** テーブルを作成したのとほぼ同じ方法である。

# COMMAND ----------

DA.daily_stream.load()       # Load another day's data
DA.process_bronze()          # Update the bronze table
DA.process_workouts_silver() # Update the workouts_silver table

# COMMAND ----------

# MAGIC %md
# MAGIC **`workouts_silver`** のデータを確認する。

# COMMAND ----------

workout_df = spark.read.table("workouts_silver")
display(workout_df)

# COMMAND ----------

# MAGIC %md
# MAGIC このデータでは、 **`user_id`** と **`session_id`** が複合キーを形成します。
# MAGIC
# MAGIC 各ペアには最終的に2つのレコードが存在し、各ワークアウトの "開始"と "停止"のアクションを示す。

# COMMAND ----------

aggregate_df = workout_df.groupby("user_id", "session_id").count()
display(aggregate_df)

# COMMAND ----------

# MAGIC %md
# MAGIC このノートブックではシャッフルをトリガーするため、シャッフル終了時のパーティション数を明示します。
# MAGIC
# MAGIC 前回と同様に、現在の並列度（最大コア数）をシャッフル・パーティションの上限値として使用できます。

# COMMAND ----------

print(f"Executor cores: {sc.defaultParallelism}")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 完了ワークアウト・テーブルの作成
# MAGIC
# MAGIC 以下のクエリは、開始アクションと停止アクションをマッチさせ、各アクションの時間を取得します。 **`in_progress`** フィールドは、ワークアウトセッションが進行中かどうかを示します。

# COMMAND ----------

def process_completed_workouts():
    spark.sql(f"""
        CREATE OR REPLACE TABLE completed_workouts 
        AS (
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
    
process_completed_workouts()

# COMMAND ----------

# MAGIC %md
# MAGIC **`completed_workouts`** テーブルに直接クエリを実行して結果を確認できるようになりました。

# COMMAND ----------

total = spark.table("completed_workouts").count() # .sql("SELECT COUNT(*) FROM completed_workouts") 
print(f"{total:3} total")

total = spark.table("completed_workouts").filter("in_progress=true").count()
print(f"{total:3} where record is still awaiting end time")

total = spark.table("completed_workouts").filter("end_time IS NOT NULL").count()
print(f"{total:3} where end time has been recorded")

total = spark.table("completed_workouts").filter("start_time IS NOT NULL").count()
print(f"{total:3} where end time arrived after start time")

total = spark.table("completed_workouts").filter("in_progress=true AND end_time IS NULL").count()
print(f"{total:3} where they are in_progress AND have an end_time")

# COMMAND ----------

# MAGIC %md
# MAGIC 以下の関数を使用して、パイプラインを通じてレコードの別のバッチをこのポイントに伝播する。

# COMMAND ----------

DA.daily_stream.load()       # Load another day's data
DA.process_bronze()          # Update the bronze table
DA.process_workouts_silver() # Update the workouts_silver table

process_completed_workouts() # Update the completed_workouts table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC AS total 
# MAGIC FROM completed_workouts

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
