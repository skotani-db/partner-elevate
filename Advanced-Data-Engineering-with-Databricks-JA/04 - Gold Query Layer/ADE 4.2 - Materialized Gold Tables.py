# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # マテリアライズドゴールドテーブル
# MAGIC
# MAGIC レイクハウスは、オンデマンドの計算リソースと無限にスケーラブルなクラウドオブジェクトストレージを組み合わせることで、コストとパフォーマンスを最適化しています。そのため、マテリアライズドビューのコンセプトは、ゴールドテーブルのコンセプトに最も近いものです。ビューの結果をクイックにアクセスするためにキャッシュするのではなく、結果は効率的なデシリアライズのためにDelta Lakeに格納されます。
# MAGIC
# MAGIC 注意：Databricks SQLでは、<a href="https://docs.databricks.com/sql/admin/query-caching.html#query-caching" target="_blank">Deltaキャッシング</a>とクエリキャッシングを活用しているため、クエリの後続の実行はキャッシュされた結果を使用します。
# MAGIC
# MAGIC ゴールドテーブルとは、Delta Lakeに永続化されたデータの非常に洗練された、一般的には集計されたビューを指します。
# MAGIC
# MAGIC これらのテーブルは、主要なビジネスロジック、ダッシュボード、およびアプリケーションの推進を目的としています。
# MAGIC
# MAGIC ゴールドテーブルの必要性は時間とともに進化します。より多くのアナリストやデータサイエンティストがレイクハウスを使用するようになると、クエリの履歴を分析することで、データがどのようにクエリされるか、いつ、誰によってクエリされるかといったトレンドが明らかになります。チーム間で協力し、データエンジニアとプラットフォーム管理者は、高い価値を持つデータをチームに適時に提供するためのSLAを定義することができます。これにより、より大きなアドホッククエリに関連する潜在的なコストとレイテンシを削減できます。
# MAGIC
# MAGIC このノートブックでは、各完了したワークアウトに関する要約統計と、ビン分割されたデモグラフィック情報を格納するゴールドテーブルを作成します。このようにして、アプリケーションは他のユーザーが同じワークアウトでどのようにパフォーマンスを発揮したかについての統計情報を素早く取得できます。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_bpm_summary.png" width="60%" />
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの最後に、学生は以下のことができるようになります：
# MAGIC
# MAGIC - ビューとテーブルのパフォーマンスの違いを説明する
# MAGIC - ストリーミング集計テーブルを実装する

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC パスとチェックポイントの変数を設定します（後で使用します）。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-5.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## ワークアウトのBPMを探索する
# MAGIC 既に **`workout_bpm** テーブルは、すべての完了したワークアウトをユーザーのBPM記録にマッチングしています。
# MAGIC
# MAGIC 以下でこのデータを探索してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM workout_bpm
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ここでは、ワークアウトの要約統計量を計算する。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, workout_id, session_id, MIN(heartrate) min_bpm, MEAN(heartrate) avg_bpm, MAX(heartrate) max_bpm, COUNT(heartrate) num_recordings
# MAGIC FROM workout_bpm
# MAGIC GROUP BY user_id, workout_id, session_id

# COMMAND ----------

# MAGIC %md
# MAGIC そして今度は、 **`user_lookup`** テーブルを使って、これをビン詰めされた人口統計情報と照合することができる。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workout_id, session_id, a.user_id, age, gender, city, state, min_bpm, avg_bpm, max_bpm, num_recordings
# MAGIC FROM user_bins a
# MAGIC INNER JOIN 
# MAGIC   (SELECT user_id, workout_id, session_id, 
# MAGIC           min(heartrate) AS min_bpm, 
# MAGIC           mean(heartrate) AS avg_bpm,
# MAGIC           max(heartrate) AS max_bpm, 
# MAGIC           count(heartrate) AS num_recordings
# MAGIC    FROM workout_bpm
# MAGIC    GROUP BY user_id, workout_id, session_id) b
# MAGIC ON a.user_id = b.user_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## インクリメンタルバッチテーブル更新の実行
# MAGIC **`workout_bpm`** テーブルは Append-onlyのストリームとして書かれているので、ストリーミングジョブを使って集計を更新することもできる。

# COMMAND ----------

(spark.readStream
      .table("workout_bpm")
      .createOrReplaceTempView("TEMP_workout_bpm"))

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake で trigger-available-now ロジックを使用すると、上流のソース・テーブルでレコードが変更された場合のみ、新しい結果を計算するようにできます。

# COMMAND ----------

user_bins_df = spark.sql("""
    SELECT workout_id, session_id, a.user_id, age, gender, city, state, min_bpm, avg_bpm, max_bpm, num_recordings
    FROM user_bins a
    INNER JOIN
      (SELECT user_id, workout_id, session_id, 
              min(heartrate) AS min_bpm, 
              mean(heartrate) AS avg_bpm, 
              max(heartrate) AS max_bpm, 
              count(heartrate) AS num_recordings
       FROM TEMP_workout_bpm
       GROUP BY user_id, workout_id, session_id) b
    ON a.user_id = b.user_id
    """)

(user_bins_df
     .writeStream
     .format("delta")
     .option("checkpointLocation", f"{DA.paths.checkpoints}/workout_bpm_summary")
     .option("path", f"{DA.paths.user_db}/workout_bpm_summary.delta")
     .outputMode("complete")
     .trigger(availableNow=True)
     .table("workout_bpm_summary")
     .awaitTermination())

# COMMAND ----------

# MAGIC %md
# MAGIC ## クエリ結果
# MAGIC
# MAGIC ビューを定義するのではなく、ゴールド・テーブルの更新をスケジューリングする主な利点は、結果の実体化に関連するコストを制御できることです。
# MAGIC
# MAGIC このテーブルから結果を返すと、 **`workout_bpm_summary`** テーブルをスキャンするためにいくらかの計算を使用しますが、この設計により、このデータが参照されるたびに複数のテーブルからファイルをスキャンして結合する必要がなくなります。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workout_bpm_summary

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
