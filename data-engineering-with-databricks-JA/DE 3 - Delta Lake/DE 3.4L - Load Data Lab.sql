-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-02080b66-d11c-47fb-a096-38ce02af4dbb
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC # データの読み込みラボ
-- MAGIC
-- MAGIC このラボでは、新しい Delta テーブルと既存の Delta テーブルにデータを読み込みます。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このラボの最後に、以下のことができるようになります：
-- MAGIC - 提供されたスキーマを使用して空の Delta テーブルを作成する
-- MAGIC - 既存のテーブルからレコードを Delta テーブルに挿入する
-- MAGIC - ファイルから Delta テーブルを作成するために CTAS ステートメントを使用する

-- COMMAND ----------

-- DBTITLE 0,--i18n-50357195-09c5-4ab4-9d60-ca7fd44feecc
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## セットアップ
-- MAGIC
-- MAGIC セットアップを実行して、このレッスンの変数とデータセットを設定します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.6L

-- COMMAND ----------

-- DBTITLE 0,--i18n-5b20c9b2-6658-4536-b79b-171d984b3b1e
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## データ概要
-- MAGIC
-- MAGIC JSONファイルとして書き込まれた生のKafkaデータのサンプルを使用します。
-- MAGIC
-- MAGIC 各ファイルには、5秒間隔で消費されたすべてのレコードが含まれており、Kafkaスキーマ全体を複数のレコードのJSONファイルとして格納しています。
-- MAGIC
-- MAGIC テーブルのスキーマ：
-- MAGIC
-- MAGIC | フィールド  | タイプ   | 説明                 |
-- MAGIC | ----------- | -------- | -------------------- |
-- MAGIC | key         | BINARY   | **`user_id`** フィールドがキーとして使用され、セッション/クッキー情報に対応する一意の英数字フィールドです |
-- MAGIC | offset      | LONG     | これは各パーティションごとに単調に増加する一意の値です |
-- MAGIC | partition   | INTEGER  | 現在のKafkaの実装では、パーティション0と1のみを使用しています |
-- MAGIC | timestamp   | LONG     | このタイムスタンプはエポックからのミリ秒単位で記録され、プロデューサアプリケーションがパーティションにレコードを追加する時間を表します |
-- MAGIC | topic       | STRING   | Kafkaサービスは複数のトピックをホストしていますが、ここには**`clickstream`**トピックからのレコードのみが含まれています |
-- MAGIC | value       | BINARY   | これは後で説明するフルデータペイロードです。JSONとして送信されます |

-- COMMAND ----------

-- DBTITLE 0,--i18n-5140f012-898a-43ac-bed9-b7e01a916505
-- MAGIC %md
-- MAGIC
-- MAGIC ## 空のDeltaテーブルのスキーマを定義する
-- MAGIC
-- MAGIC 同じスキーマを使用して、空の管理対象Deltaテーブル **`events_raw`** を作成します。

-- COMMAND ----------

-- TODO
<FILL_IN>

-- COMMAND ----------

-- DBTITLE 0,--i18n-70f4dbf1-f4cb-4ec6-925a-a939cbc71bd5
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC テーブルが正しく作成されたことを確認するために、以下のセルを実行してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Define Schema")
-- MAGIC expected_table = lambda: spark.table("events_raw")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"events_raw\"")
-- MAGIC suite.test_equals(lambda: expected_table().count(), 0, "The table should have 0 records")
-- MAGIC
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "key", "BinaryType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "offset", "LongType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "partition", "IntegerType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "timestamp", "LongType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "topic", "StringType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "value", "BinaryType")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite

-- COMMAND ----------

-- DBTITLE 0,--i18n-7b4e55f2-737f-4996-a51b-4f18c2fc6eb7
-- MAGIC %md
-- MAGIC
-- MAGIC ## デルタテーブルに生のイベントを挿入する
-- MAGIC
-- MAGIC 抽出したデータとデルタテーブルが準備できたら、**`events_json`** テーブルからJSONレコードを新しい **`events_raw`** デルタテーブルに挿入してください。

-- COMMAND ----------

-- TODO
<FILL_IN>

-- COMMAND ----------

-- DBTITLE 0,--i18n-65ffce96-d821-4792-b545-5725814003a0
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC データが期待どおりに書き込まれたかどうかを確認するために、テーブルの内容を手動で確認してください。

-- COMMAND ----------

-- TODO
<FILL_IN>

-- COMMAND ----------

-- DBTITLE 0,--i18n-1cbff40d-e916-487c-958f-2eb7807c5aeb
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC 以下のセルを実行して、データが正しくロードされたことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Validate events_raw")
-- MAGIC expected_table = lambda: spark.table("events_raw")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"events_raw\"")
-- MAGIC suite.test_equals(lambda: expected_table().count(), 2252, "The table should have 2252 records")
-- MAGIC
-- MAGIC first_five = lambda: [r["timestamp"] for r in expected_table().orderBy(F.col("timestamp").asc()).limit(5).collect()]
-- MAGIC suite.test_sequence(first_five, [1593879303631, 1593879304224, 1593879305465, 1593879305482, 1593879305746], True, "First 5 values are correct")
-- MAGIC
-- MAGIC last_five = lambda: [r["timestamp"] for r in expected_table().orderBy(F.col("timestamp").desc()).limit(5).collect()]
-- MAGIC suite.test_sequence(last_five, [1593881096290, 1593881095799, 1593881093452, 1593881093394, 1593881092076], True, "Last 5 values are correct")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite.passed

-- COMMAND ----------

-- DBTITLE 0,--i18n-b3c62fea-b75d-41d6-8214-660f9cfa3acd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## クエリの結果からDeltaテーブルを作成する
-- MAGIC
-- MAGIC 新しいイベントデータに加えて、このコースの後半で使用する商品の詳細を提供する小さなルックアップテーブルも読み込みましょう。
-- MAGIC 以下で提供されるparquetディレクトリからデータを抽出して、**`item_lookup`** という名前の管理対応Deltaテーブルを作成するためにCTASステートメントを使用します。

-- COMMAND ----------

-- TODO
<FILL_IN> ${da.paths.datasets}/ecommerce/raw/item-lookup

-- COMMAND ----------

-- DBTITLE 0,--i18n-5a971532-0003-4665-9064-26196cd31e89
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC 以下のセルを実行して、ルックアップテーブルが正しく読み込まれたことを確認してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Validate item_lookup")
-- MAGIC expected_table = lambda: spark.table("item_lookup")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"item_lookup\"")
-- MAGIC
-- MAGIC actual_values = lambda: [r["item_id"] for r in expected_table().collect()]
-- MAGIC expected_values = ['M_PREM_Q','M_STAN_F','M_PREM_F','M_PREM_T','M_PREM_K','P_DOWN_S','M_STAN_Q','M_STAN_K','M_STAN_T','P_FOAM_S','P_FOAM_K','P_DOWN_K']
-- MAGIC suite.test_sequence(actual_values, expected_values, False, "Contains the 12 expected item IDs")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite.passed

-- COMMAND ----------

-- DBTITLE 0,--i18n-4db73493-3920-44e2-a19b-f335aa650f76
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC  
-- MAGIC このレッスンに関連するテーブルとファイルを削除するには、次のセルを実行してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
