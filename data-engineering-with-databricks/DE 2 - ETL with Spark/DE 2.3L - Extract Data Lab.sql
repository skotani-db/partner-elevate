-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-037a5204-a995-4945-b6ed-7207b636818c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC # データの抽出ラボ
-- MAGIC
-- MAGIC このラボでは、JSONファイルから生のデータを抽出します。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このラボの最後までに、次のことができるようになります：
-- MAGIC - JSONファイルからデータを抽出するために外部テーブルを登録する

-- COMMAND ----------

-- DBTITLE 0,--i18n-3b401203-34ae-4de5-a706-0dbbd5c987b7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## セットアップ実行
-- MAGIC
-- MAGIC 以下のセルを実行して、このレッスンのための変数とデータセットを設定してください。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.3L

-- COMMAND ----------

-- DBTITLE 0,--i18n-9adc06e2-7298-4306-a062-7ff70adb8032
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## データの概要
-- MAGIC
-- MAGIC この例では、JSONファイルとして書かれた生のKafkaデータのサンプルと連携します。
-- MAGIC 各ファイルには、5秒間の間に受信されたすべてのレコードが含まれており、完全なKafkaスキーマを使用して複数のレコードがJSONファイルとして保存されています。
-- MAGIC
-- MAGIC テーブルのスキーマ：
-- MAGIC
-- MAGIC | フィールド | タイプ | 説明 |
-- MAGIC | ------ | ---- | ----------- |
-- MAGIC | key    | BINARY | **`user_id`** フィールドはキーとして使用されます。これはセッション/クッキー情報に対応するユニークな英数字フィールドです |
-- MAGIC | offset | LONG | これは各パーティションごとに単調に増加するユニークな値です |
-- MAGIC | partition | INTEGER | 現在のKafka実装では、2つのパーティション（0と1）のみを使用しています |
-- MAGIC | timestamp | LONG | このタイムスタンプはエポックからのミリ秒として記録され、プロデューサがレコードをパーティションに追加する時刻を表します |
-- MAGIC | topic | STRING | Kafkaサービスは複数のトピックをホストしていますが、ここには**`clickstream`**トピックからのレコードのみが含まれています |
-- MAGIC | value | BINARY | これは後で詳しく説明するフルデータペイロードで、JSONとして送信されます |

-- COMMAND ----------

-- DBTITLE 0,--i18n-8cca978a-3034-4339-8e6e-6c48d389cce7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC  
-- MAGIC ## JSONファイルから生のイベントを抽出
-- MAGIC JSONデータを適切にDeltaにロードするために、まず正しいスキーマを使用してJSONデータを抽出する必要があります。
-- MAGIC 以下のファイルパスにあるJSONファイルに対して外部テーブルを作成します。このテーブルの名前は events_json とし、上記のスキーマを宣言します。

-- COMMAND ----------

-- TODO
<FILL_IN> "${DA.paths.kafka_events}" 

-- COMMAND ----------

-- DBTITLE 0,--i18n-33231985-3ff1-4f44-8098-b7b862117689
-- MAGIC %md
-- MAGIC
-- MAGIC **注意**: このラボの途中で、確認を実行するためにPythonを使用します。以下のセルは、指示に従っていない場合にエラーとメッセージを返します。セルの実行結果がない場合、このステップを完了していることを意味します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_json"), "Table named `events_json` does not exist"
-- MAGIC assert spark.table("events_json").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_json").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC
-- MAGIC total = spark.table("events_json").count()
-- MAGIC assert total == 2252, f"Expected 2252 records, found {total}"

-- COMMAND ----------

-- DBTITLE 0,--i18n-6919d58a-89e4-4c02-812c-98a15bb6f239
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC  以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
