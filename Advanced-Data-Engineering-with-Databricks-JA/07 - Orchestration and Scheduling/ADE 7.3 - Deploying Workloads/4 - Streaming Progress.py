# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # ストリーミングの進行状況のモニタリング
# MAGIC
# MAGIC このノートブックは、カスタムの[StreamingQueryListener]($../../Includes/StreamingQueryListener)が書き込んだJSONログを増分的にロードするように設定されています。
# MAGIC
# MAGIC ストリーミングとバッチの操作と併用して小さなクラスタで実行すると、大幅な遅延が発生する可能性がありますのでご注意ください。
# MAGIC
# MAGIC 理想的には、すべてのジョブは分離されたジョブクラスタでスケジュールするべきです。

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.4.4

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のコードは、Auto Loaderを使用してログデータを増分的にDelta Lakeテーブルにロードするものです。

# COMMAND ----------

query = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/streaming_logs")
              .load(DA.paths.streaming_logs_json)
              .writeStream
              .option("mergeSchema", True)
              .option("checkpointLocation", f"{DA.paths.checkpoints}/streaming_logs")
              .trigger(availableNow=True)
              .start(DA.paths.streaming_logs_delta))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 以下でストリーミングログをクエリします

# COMMAND ----------

display(spark.read.load(DA.paths.streaming_logs_delta))

# COMMAND ----------

# MAGIC %md
# MAGIC 他のレッスンとは異なり、このデモでは **`DA.cleanup()`** コマンドを実行しません。
# MAGIC
# MAGIC このデモのすべてのノートブックでこれらのアセットを維持したいためです。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
