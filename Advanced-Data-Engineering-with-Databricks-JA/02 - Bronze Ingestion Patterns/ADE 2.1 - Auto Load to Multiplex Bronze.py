# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Auto Load Data to Multiplex Bronze
# MAGIC
# MAGIC チーフアーキテクトは、Kafkaに直接接続するのではなく、ソースシステムが生のレコードをJSONファイルとしてクラウドオブジェクトストレージに送信することを決定しました。このノートブックでは、Auto Loader でこれらのレコードを取り込み、このインクリメンタルフィードの全履歴を保存するマルチプレックステーブルを構築します。初期テーブルには、すべてのトピックのデータが格納され、次のスキーマを持ちます。
# MAGIC
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | key | BINARY |
# MAGIC | value | BINARY |
# MAGIC | topic | STRING |
# MAGIC | partition | LONG |
# MAGIC | offset | LONG
# MAGIC | timestamp | LONG |
# MAGIC | date | DATE |
# MAGIC | week_part | STRING |
# MAGIC
# MAGIC この1つのテーブルが、ターゲット・アーキテクチャを通じてデータの大部分を動かし、相互に依存する3つのデータ・パイプラインに供給する。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_bronze.png" width="60%" />
# MAGIC
# MAGIC **NOTE**: Kafkaに接続するための追加設定の詳細については、以下をご覧ください。 <a href="https://docs.databricks.com/spark/latest/structured-streaming/kafka.html" target="_blank">here</a>.
# MAGIC
# MAGIC
# MAGIC ## このレッスンの終わりまでに、以下のことができるようになります：
# MAGIC - マルチプレックス設計の説明
# MAGIC - オートローダを適用してレコードをインクリメンタルに処理する
# MAGIC - トリガ間隔の構成
# MAGIC - "trigger-available-now"ロジックを使用して、データのトリガーによるインクリメンタルローディングを実行する

# COMMAND ----------

# MAGIC %md
# MAGIC 次のセルは、このノートブック全体で必要なパスを宣言している。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-3.1

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"> このトレーニングの例では、すべてのレコードは DBFS ルートに保存されます。
# MAGIC
# MAGIC 開発環境でも本番環境でも、データのレイヤーごとに別々のデータベースとストレージアカウントを設定することが望ましい。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ソース・データの検査
# MAGIC
# MAGIC データファイルは以下の変数で指定されたパスに書き込まれる。
# MAGIC
# MAGIC 以下のセルを実行して、ソース・データのスキーマを調べ、取り込まれる際に変更が必要かどうかを判断してください。

# COMMAND ----------

spark.read.json(DA.paths.source_daily).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 日付ルックアップテーブルで結合するデータを準備する
# MAGIC 初期化スクリプトは **`date_lookup`** テーブルをロードした。このテーブルにはあらかじめ計算された日付の値がいくつもある。休日や会計四半期を示す追加のフィールドが、後でデータを充実させるためにこのテーブルに追加されることがよくあることに注意してください。
# MAGIC
# MAGIC これらの値の事前計算と保存は、文字列パターン **`YYYY-WW`** を使用して、年と週でデータをパーティショニングしたい場合に特に重要です。Sparkには、 **`year`** と **`weekofyear`** の両方の関数が組み込まれていますが、 **`year`** と **`weekofyear`** の両方の関数を使用することはできません。 
# MAGIC  **`weekofyear`** 関数は、12月最終週や12月最終週、<a href="https://spark.apache.org/docs/2.3.0/api/sql/#weekofyear" target="_blank">1月最初の週</a>の日付に対して期待される動作を提供しない可能性があります。3日以上ある最初の週を第1週と定義しているためです。
# MAGIC
# MAGIC このエッジケースはSparkにとっては難解ですが、組織全体で使用される　**`date_lookup`** テーブルは、データが一貫して日付関連の詳細で充実していることを確認するために重要です。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE date_lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from date_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC 現在実装されているテーブルでは、各 **`date`** に対して正確な **`week_part`** を取得する必要があります。
# MAGIC
# MAGIC 以下の呼び出しは、後続の結合操作に必要な **`DataFrame`** を作成します。

# COMMAND ----------

date_lookup_df = spark.table("date_lookup").select("date", "week_part")

# COMMAND ----------

display(date_lookup_df)

# COMMAND ----------

# MAGIC %md
# MAGIC  **`DA.paths.source_daily`** に格納されているJSONデータを使用して、必要に応じて **`timestamp`** カラムを変換し、 **`date`** カラムと結合します。

# COMMAND ----------

json_df = spark.read.json(DA.paths.source_daily)
display(json_df)

# COMMAND ----------

# TODO
from pyspark.sql import functions as F
json_df = spark.read.json(DA.paths.source_daily)
 
joined_df = (json_df.join(F.broadcast(date_lookup_df),
                          F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date"),
                          "left"))
 
display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 多重ブロンズテーブルへのトリガーインクリメンタルオートローディングの定義
# MAGIC
# MAGIC 以下は、ソース・ディレクトリからブロンズ・テーブルへのデータをインクリメンタルに処理し、最初の書き込み中にテーブルを作成する関数のスターター・コードです。
# MAGIC
# MAGIC 不足しているコードを以下に示します：
# MAGIC - Auto Loader を使用するようにストリームを構成する
# MAGIC - JSON 形式を使用するように Auto Loader を構成する。
# MAGIC - date_lookupテーブルとブロードキャスト結合を実行する。
# MAGIC -  **`topic`** フィールドと **`week_part`** フィールドでデータをパーティションする。

# COMMAND ----------

# TODO
def process_bronze():
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "json")
                  .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/bronze_schema")
                  .load(DA.paths.source_daily)
                  .join(F.broadcast(date_lookup_df), F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date"), "left")
                  .writeStream
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
                  .partitionBy("topic", "week_part")
                  .trigger(availableNow=True)
                  .table("bronze"))
 
    query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のセルを実行して、データの増分バッチを処理する。

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC 処理されたレコードの数を確認する。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC データをプレビューし、レコードが正しく取り込まれていることを確認する。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 以下の　**`DA.daily_stream.load()`** コードは、ソース・ディレクトリに新しいデータを取り込むためのヘルパー・クラスです。
# MAGIC
# MAGIC 以下のセルを実行すると、新しいバッチが正常に処理されます。

# COMMAND ----------

DA.daily_stream.load()

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm the count is now higher.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

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
