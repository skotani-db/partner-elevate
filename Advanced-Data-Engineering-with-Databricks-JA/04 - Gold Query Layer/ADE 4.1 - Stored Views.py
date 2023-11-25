# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # ストアド・ビュー
# MAGIC
# MAGIC このノートブックでは、ストアド・ビューの作成方法と管理方法について簡単に説明します。ストアド・ビューは、データベースに永続化される（他のユーザが事前に定義されたロジックを利用して結果を具体化できる）点で、DataFramesやtemp viewとは異なることを思い出してください。ビューは、結果を計算するために必要なロジックを登録します。デルタ湖ソースに対して定義されたビューは、常に各データソースの最新バージョンにクエリすることが保証されています。
# MAGIC
# MAGIC このノートブックの目標は、パートナーのジムのアナリストが、Moovio デバイスの使用とワークアウトがジムのアクティビティにどのような影響を与えるかを調査するためのビューを生成することです。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_gym_report.png" width="60%" />
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、受講者は以下のことができるようになります：
# MAGIC - ビューに関連するクエリプランを表示する
# MAGIC - Delta Lakeテーブルからどのように結果がマテリアライズされるか説明する

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-5.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## ジムのログを調べる
# MAGIC
# MAGIC ジムのログのスキーマを見直すことから始めましょう。

# COMMAND ----------

gymDF = spark.table("gym_mac_logs")
gymDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark DataFrame とビューはほぼ同じ構造体です。DataFrame に対して **`explain`** を呼び出すことで、ソーステーブルがデータを含むファイルをデシリアライズするための命令セットであることがわかります。

# COMMAND ----------

gymDF.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ワークアウトデータを調べる
# MAGIC
# MAGIC 可能な限りのメトリックをビューに取り込むのではなく、ジムのアナリストが興味を持ちそうな値のサマリーを作成します。
# MAGIC
# MAGIC 私たちがジムから受け取っているデータは、マックアドレスで示されるユーザーデバイスの記録された最初と最後のタイムスタンプを示しています。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gym_mac_logs

# COMMAND ----------

# MAGIC %md
# MAGIC ## クエリの作成
# MAGIC
# MAGIC **`completed_workouts`** テーブルは、各ユーザーのワークアウトの開始時刻と終了時刻を示しています。
# MAGIC
# MAGIC 以下のセルを使用して、以下のクエリを作成します：
# MAGIC - ユーザが少なくとも 1 つのワークアウトを完了した各日付
# MAGIC - ワークアウトの最も早い開始時刻
# MAGIC - 各日のワークアウトの最新の **`end_time`** 時間
# MAGIC - ユーザが毎日完了したすべてのワークアウトのリスト

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC SELECT user_id, to_date(start_time) date, collect_set(workout_id), min(start_time), max(end_time)
# MAGIC FROM completed_workouts
# MAGIC GROUP BY user_id, to_date(start_time)

# COMMAND ----------

# MAGIC %md
# MAGIC ## クエリーの拡張
# MAGIC
# MAGIC さて、このデータをジムから送られてきたMACログと結合して、ビューを作成します。
# MAGIC
# MAGIC **`user_lookup`** テーブルから取得できる識別子として、 **`mac_address`** を保持します。
# MAGIC
# MAGIC また、ユーザーがジムを訪問している間の総経過分数と、最初のワークアウトの開始から最後のワークアウトの終了までの総経過分数を計算するためのカラムを追加します。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT gym, mac_address, date, workouts, (last_timestamp - first_timestamp)/60 minutes_in_gym, (to_unix_timestamp(end_workout) - to_unix_timestamp(start_workout))/60 minutes_exercising
# MAGIC FROM gym_mac_logs c
# MAGIC INNER JOIN (
# MAGIC   SELECT b.mac_address, to_date(start_time) date, collect_set(workout_id) workouts, min(start_time) start_workout, max(end_time) end_workout
# MAGIC       FROM completed_workouts a
# MAGIC       INNER JOIN user_lookup b
# MAGIC       ON a.user_id = b.user_id
# MAGIC       GROUP BY mac_address, to_date(start_time)
# MAGIC   ) d
# MAGIC   ON c.mac = d.mac_address AND to_date(CAST(c.first_timestamp AS timestamp)) = d.date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 最終ロジックにビューを登録する
# MAGIC
# MAGIC 上記のクエリを使用して、 **`gym_user_stats`** という（一時的でない）ビューを作成します。
# MAGIC
# MAGIC **`CREATE VIEW IF NOT EXISTS gym_user_stats AS (...)`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC CREATE VIEW IF NOT EXISTS gym_user_stats AS (
# MAGIC   SELECT gym, mac_address, date, workouts, (last_timestamp - first_timestamp)/60 minutes_in_gym, (to_unix_timestamp(end_workout) - to_unix_timestamp(start_workout))/60 minutes_exercising
# MAGIC FROM gym_mac_logs c
# MAGIC INNER JOIN (
# MAGIC   SELECT b.mac_address, to_date(start_time) date, collect_set(workout_id) workouts, min(start_time) start_workout, max(end_time) end_workout
# MAGIC       FROM completed_workouts a
# MAGIC       INNER JOIN user_lookup b
# MAGIC       ON a.user_id = b.user_id
# MAGIC       GROUP BY mac_address, to_date(start_time)
# MAGIC   ) d
# MAGIC   ON c.mac = d.mac_address AND to_date(CAST(c.first_timestamp AS timestamp)) = d.date
# MAGIC   )

# COMMAND ----------

# Check your work
assert spark.sql("SHOW TABLES").filter("tableName='gym_user_stats'").count() >= 1, "View 'gym_user_stats' does not exist."
assert spark.sql("SHOW TABLES").filter("tableName='gym_user_stats'").first()["isTemporary"]==False, "View 'gym_user_stats' should be not temporary."
assert spark.sql("DESCRIBE EXTENDED gym_user_stats").filter("col_name='Type'").first()['data_type']=='VIEW', "Found a table 'gym_user_stats' when a view was expected."
assert spark.table("gym_user_stats").count() == 304, "Incorrect query used for view 'gym_user_stats'."
print("All tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ビューは単にクエリのスパークプランを保存していることがわかります。

# COMMAND ----------

spark.table("gym_user_stats").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC このビューに対してクエリを実行すると、プランを処理して論理的に正しい結果を生成します。
# MAGIC
# MAGIC データはデルタキャッシュに格納されるかもしれませんが、この結果は永続化される保証はなく、現在アクティブなクラスタに対してのみキャッシュされることに注意してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gym_user_stats
# MAGIC WHERE gym = 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## ACL に関する余談
# MAGIC
# MAGIC DatabricksはACLを広範囲にサポートしていますが、デフォルトでは標準的なデータエンジニアリングクラスタには適用されません。そのためSQL Warehouseで実行することが代替案です。

# COMMAND ----------

import py4j

try:
    spark.sql("SHOW GRANT ON VIEW gym_user_stats")
    
except py4j.protocol.Py4JJavaError as e:
    print("Error: " + e.java_exception.getMessage())
    print("Solution: Consider enabling Table Access Control to demonstrate this feature.")

# COMMAND ----------

# MAGIC %md
# MAGIC このビューの権限は特に重要ではありませんが、ブロンズテーブル（すべての生データを含む）も現在この方法で保存されていることがわかります。
# MAGIC
# MAGIC 繰り返しますが、ACL は主に BI やデータサイエンスのユースケースで Databricks ワークスペース内のデータアクセスを管理するためのものです。機密性の高いデータエンジニアリングデータについては、クラウドアイデンティティ管理を使用してストレージコンテナへのアクセスを制限することをお勧めします。

# COMMAND ----------

try:
    spark.sql("SHOW GRANT ON TABLE bronze")
    
except py4j.protocol.Py4JJavaError as e:
    print("Error: " + e.java_exception.getMessage())
    print("Solution: Consider enabling Table Access Control to demonstrate this feature.")

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
