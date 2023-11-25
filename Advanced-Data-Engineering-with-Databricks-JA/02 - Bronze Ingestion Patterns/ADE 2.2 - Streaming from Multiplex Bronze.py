# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 多重ブロンズからのストリーミング
# MAGIC
# MAGIC このノートブックでは、前回のレッスンで設定した multiplex bronze テーブルにある単一のトピックから生データを取得し、解析するクエリを設定します。次のノートブックでは、このクエリをさらに改良していきます。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、以下のことができるようになる：
# MAGIC - ストリーミングジョブにフィルタが適用される方法を説明する
# MAGIC - 組み込み関数を使用して、入れ子になった JSON データを平坦化する
# MAGIC - バイナリエンコードされた文字列をネイティブ型にパースして保存する

# COMMAND ----------

# MAGIC %md
# MAGIC データベースを宣言し、すべてのパス変数を設定する。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-3.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## バッチ読み取りの定義
# MAGIC
# MAGIC ストリームを構築する前に、データの静的ビューから始めます。ストリームがトリガーされないため、インタラクティブな開発では、静的なデータで作業する方が簡単です。
# MAGIC
# MAGIC ソースとして Delta Lake を使用しているため、クエリを実行するたびに、テーブルの最新バージョンを取得できます。
# MAGIC
# MAGIC SQL を使用している場合は、前のレッスンで登録した **`bronze`** テーブルに直接クエリを実行できます。
# MAGIC
# MAGIC PythonやScalaのユーザーは、登録したテーブルから簡単にDataframeを作成することができます。

# COMMAND ----------

batch_df = spark.table("bronze")
display(batch_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake にはスキーマ情報が保存されている。
# MAGIC
# MAGIC その構造を思い出すために、プリントアウトしてみよう：

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze

# COMMAND ----------

# MAGIC %md
# MAGIC データをプレビューする。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC インジェストされるトピックは複数ある。そのため、それぞれのトピックに対して個別にロジックを定義する必要がある。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC バイナリ・フィールドを文字列としてキャストする。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 心拍数の記録を解析する
# MAGIC
# MAGIC 心拍数の記録を解析するロジックを定義することから始めよう。このロジックは静的データに対して記述します。Structured Streamingでは<a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" target="_blank">サポートされていないオペレーション</a>がいくつかあるので、これらの制限を念頭に置いて現在のクエリを構築しなければ、ロジックの一部をリファクタリングする必要があるかもしれないことに注意してください。
# MAGIC
# MAGIC  **`bpm`** トピックを以下のスキーマにパースする1つのクエリを繰り返し開発します。
# MAGIC
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | device_id | LONG | 
# MAGIC | time | TIMESTAMP | 
# MAGIC | heartrate | DOUBLE |
# MAGIC
# MAGIC アーキテクチャ図の **`heartrate_silver`** テーブルを作成します。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_heartrate_silver.png" width="60%" />

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "device_id LONG, time TIMESTAMP, heartrate DOUBLE") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "bpm")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ストリーミング読み取りのための変換ロジック
# MAGIC
# MAGIC Deltaテーブルに対して直接ストリーミング読み込みを定義することができます。ストリーミングクエリのほとんどの設定は、読み込みではなく書き込みで行われることに注意してください。
# MAGIC
# MAGIC 以下のセルは、静的テーブルをストリーミング・テンポ・ビューに変換する方法を示しています（Spark SQLでストリーミング・クエリを書きたい場合）。

# COMMAND ----------

(spark.readStream
      .table("bronze")
      .createOrReplaceTempView("TEMP_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC この一時ビューを参照するように上記のクエリを更新すると、ストリーミングの結果が得られる。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "device_id LONG, time TIMESTAMP, heartrate DOUBLE") v
# MAGIC   FROM TEMP_bronze
# MAGIC   WHERE topic = "bpm")

# COMMAND ----------

# MAGIC %md
# MAGIC ストリーミング読み込みがノートブックに表示されるたびに、ストリーミング・ジョブが開始されます。 ストリームを停止するには、上のセルの「キャンセル」リンクをクリックするか、ノートブックの上部にある「実行停止」をクリックするか、以下のコードを実行してください。
# MAGIC
# MAGIC 続行する前に、上記のストリーミング表示を停止してください。

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()
    stream.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 結果をディスクに永続化するには、Pythonを使ってストリーミング書き込みを行う必要があります。 SQLからPythonに切り替えるには、適用したいクエリを取り込むための仲介としてtemporary viewを使用します。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW TEMP_SILVER AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "device_id LONG, time TIMESTAMP, heartrate DOUBLE") v
# MAGIC     FROM TEMP_bronze
# MAGIC     WHERE topic = "bpm")

# COMMAND ----------

# MAGIC %md
# MAGIC ストリーミングの **`TEMP_SILVER`** 一時ビューから読み込み、 **`heart_rate_silver`** デルタテーブルに書き込む。
# MAGIC
# MAGIC  **`trigger(availableNow=True)`** オプションを使用すると、データがなくなるまですべてのレコードを（必要であれば複数のバッチで）処理し、その後ストリームを停止する。

# COMMAND ----------

query = (spark.table("TEMP_SILVER").writeStream
               .option("checkpointLocation", f"{DA.paths.checkpoints}/heart_rate")
               .option("path", f"{DA.paths.user_db}/heart_rate_silver.delta")
               .trigger(availableNow=True)
               .table("heart_rate_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC SQLを使う代わりに、Python Dataframes APIを使ってジョブ全体を表現することもできます。 以下のセルはこのロジックをPythonにリファクタリングしたものです。

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

(spark
   .readStream.table("bronze")
   .filter("topic = 'bpm'")
   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
   .select("v.*")
   .writeStream
       .option("checkpointLocation", f"{DA.paths.checkpoints}/heart_rate")
       .option("path", f"{DA.paths.user_db}/heart_rate_silver.delta")
       .trigger(availableNow=True)
       .table("heart_rate_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> 続行する前に、ストリームをすべてキャンセルしてください。画面上部の **`Run All`** ボタンは、ストリームがまだ実行されている場合は **`Stop Execution`** と表示されます。 または以下のコードを実行して、このクラスタで実行中のストリームをすべて停止します。

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()
    stream.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## シルバー・テーブルの動機
# MAGIC
# MAGIC レコードのパース、スキーマのflattenと変更に加えて、シルバーテーブルに書き込む前にデータの品質をチェックする必要があります。
# MAGIC
# MAGIC 以下のノートブックでは、さまざまな品質チェックについて説明します。

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
