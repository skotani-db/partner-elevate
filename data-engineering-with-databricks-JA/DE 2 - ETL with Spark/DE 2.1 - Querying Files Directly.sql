-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-a0d28fb8-0d0f-4354-9720-79ce468b5ea8
-- MAGIC %md
-- MAGIC
-- MAGIC # Spark SQLを使用してファイルからデータを直接抽出する
-- MAGIC
-- MAGIC このノートブックでは、DatabricksでSpark SQLを使用してファイルからデータを直接抽出する方法を学びます。
-- MAGIC
-- MAGIC このオプションは多くのファイル形式でサポートされていますが、自己記述型のデータ形式（ParquetやJSONなど）に最も役立ちます。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このレッスンの最後までに、以下のことができるようになるはずです：
-- MAGIC - Spark SQLを使用してデータファイルを直接クエリする
-- MAGIC - ビューとCTEをレイヤー化してデータファイルへの参照を容易にする
-- MAGIC - 生のファイル内容を確認するために**`text`**および**`binaryFile`**メソッドを活用する

-- COMMAND ----------

-- DBTITLE 0,--i18n-73162404-8907-47f6-9b3e-dd17819d71c9
-- MAGIC %md
-- MAGIC
-- MAGIC ## セットアップの実行
-- MAGIC
-- MAGIC セットアップスクリプトを実行すると、データが作成され、このノートブックの残りの部分を実行するために必要な値が宣言されます。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.1

-- COMMAND ----------

-- DBTITLE 0,--i18n-480bfe0b-d36d-4f67-8242-6a6d3cca38dd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## データ概要
-- MAGIC
-- MAGIC この例では、JSONファイルとして書き込まれた生のKafkaデータのサンプルを使用します。
-- MAGIC
-- MAGIC 各ファイルには、5秒間隔で消費されたすべてのレコードが含まれ、完全なKafkaスキーマを使用した複数のレコードのJSONファイルとして保存されています。
-- MAGIC
-- MAGIC | フィールド | タイプ | 説明 |
-- MAGIC | --- | --- | --- |
-- MAGIC | key | BINARY | **`user_id`** フィールドはキーとして使用されます。これはセッション/クッキー情報に対応するユニークな英数字フィールドです。 |
-- MAGIC | value | BINARY | これは後で説明するように、JSONとして送信されるデータのペイロード全体です。 |
-- MAGIC | topic | STRING | Kafkaサービスは複数のトピックをホストしていますが、ここに含まれているのは **`clickstream`** トピックからのレコードのみです。 |
-- MAGIC | partition | INTEGER | 現在のKafka実装では、2つのパーティション（0と1）のみが使用されています。 |
-- MAGIC | offset | LONG | これは各パーティションごとに単調に増加する一意の値です。 |
-- MAGIC | timestamp | LONG | このタイムスタンプはエポックからのミリ秒として記録され、プロデューサーアプリがパーティションにレコードを追加する時間を表します。 |

-- COMMAND ----------

-- DBTITLE 0,--i18n-65941466-ca87-4c29-903e-658e24e48cee
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC **注意:** 弊社のソースディレクトリには多くのJSONファイルが含まれていることに注意してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(DA.paths.kafka_events)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(DA.paths.kafka_events)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-f1ddfb40-9c95-4b9a-84e5-2958ac01166d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ここでは、DBFSルートに書き込まれたデータに対して相対ファイルパスを使用します。
-- MAGIC ほとんどのワークフローでは、ユーザーは外部クラウドストレージの場所からデータにアクセスする必要があります。
-- MAGIC ほとんどの企業では、ワークスペースの管理者がこれらのストレージ場所へのアクセスを設定する責任があります。
-- MAGIC これらの場所を構成しアクセスする手順については、「クラウドベンダー固有の自己学習コース」の中にある「クラウドアーキテクチャとシステム統合」のタイトルで詳細が記載されています。
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-9abfecfc-df3f-4697-8880-bd3f0b58a864
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## 単一ファイルのクエリ
-- MAGIC
-- MAGIC 単一ファイルに含まれるデータをクエリするには、次のパターンでクエリを実行します：
-- MAGIC
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>
-- MAGIC
-- MAGIC パスの周りにバックティック（単一引用符ではなく）が使用されていることに特に注意してください。
-- MAGIC

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c2891f1-e055-4fde-8bf9-3f448e4cdb2b
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC **注意:** 弊社のプレビューでは、ソースファイルのすべての321行が表示されます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-0f45ecb7-4024-4798-a9b8-e46ac939b2f7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## ファイルディレクトリのクエリ
-- MAGIC
-- MAGIC ディレクトリ内のすべてのファイルが同じフォーマットとスキーマを持つと仮定し、個々のファイルではなくディレクトリパスを指定することで、すべてのファイルを同時にクエリできます。

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-6921da25-dc10-4bd9-9baa-7e589acd3139
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC デフォルトでは、このクエリは最初の1000行のみ表示します。

-- COMMAND ----------

-- DBTITLE 0,--i18n-035ddfa2-76af-4e5e-a387-71f26f8c7f76
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## ファイルへの View の作成
-- MAGIC ファイルとディレクトリへの直接のクエリが可能であるため、ファイルに対するクエリに対して追加のSparkロジックを連結できます。
-- MAGIC パスに対するクエリからビューを作成すると、後のクエリでこのビューを参照できます。

-- COMMAND ----------

CREATE OR REPLACE VIEW event_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c29b73b-b4b0-48ab-afbb-7b1422fce6e4
-- MAGIC %md
-- MAGIC
-- MAGIC ビューへのアクセス許可と基礎ストレージ場所へのアクセス許可がある限り、ユーザーはこのビュー定義を使用して基礎データをクエリできます。これはワークスペース内の異なるユーザー

-- COMMAND ----------

SELECT * FROM event_view

-- COMMAND ----------

-- DBTITLE 0,--i18n-efd0c0fc-5346-4275-b083-4ee96ce8a852
-- MAGIC %md
-- MAGIC ## 一時的なファイルへの View の作成
-- MAGIC 一時ビューは、同様にクエリを名前

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-a9f9827b-2258-4481-a9d9-6fecf55aeb9b
-- MAGIC %md
-- MAGIC
-- MAGIC 一時ビューは、現在のSparkセッションにのみ存在します。Databricksの場合、これは現在のノートブック、ジョブ、またはDBSQLクエリに対して分離されています。

-- COMMAND ----------

SELECT * FROM events_temp_view

-- COMMAND ----------

-- DBTITLE 0,--i18n-dcfaeef2-0c3b-4782-90a6-5e0332dba614
-- MAGIC %md
-- MAGIC ## クエリ内での参照のためのCTEの適用
-- MAGIC 共通テーブル表現（CTE）は、クエリの結果に対する短期間で人間が読みやすい参照が必要な場合に最適です。

-- COMMAND ----------

WITH cte_json
AS (SELECT * FROM json.`${DA.paths.kafka_events}`)
SELECT * FROM cte_json

-- COMMAND ----------

-- DBTITLE 0,--i18n-c85e1553-f643-47b8-b909-0d10d2177437
-- MAGIC %md
-- MAGIC CTEは、クエリが計画および実行されている間にのみクエリの結果にエイリアスを付けます。
-- MAGIC
-- MAGIC したがって、**次のセルを実行するとエラーが発生します**。

-- COMMAND ----------

-- SELECT COUNT(*) FROM cte_json

-- COMMAND ----------

-- DBTITLE 0,--i18n-106214eb-2fec-4a27-b692-035a86b8ec8d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## テキストファイルを生の文字列として抽出
-- MAGIC
-- MAGIC テキストベースのファイル（JSON、CSV、TSV、TXT形式を含む）で作業する場合、**`text`** フォーマットを使用してファイルの各行を1つの文字列列である **`value`** という名前の行としてロードできます。これは、データソースが破損しやすく、カスタムテキスト解析関数を使用してテキストフィールドから値を抽出する必要がある場合に便利です。
-- MAGIC

-- COMMAND ----------

SELECT * FROM text.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-732e648b-4274-48f4-86e9-8b42fd5a26bd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## ファイルの生のバイトとメタデータを抽出
-- MAGIC
-- MAGIC 一部のワークフローでは、画像や非構造化データを扱う場合など、ファイル全体と連携する必要があることがあります。ディレクトリをクエリするために **`binaryFile`** を使用すると、ファイルの内容と並行してファイルのメタデータが提供されます。
-- MAGIC
-- MAGIC 具体的には、生成されるフィールドには **`path`**、**`modificationTime`**、**`length`**、および **`content`** が示されます。

-- COMMAND ----------

SELECT * FROM binaryFile.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-9ac20d39-ae6a-400e-9e13-14af5d4c91df
-- MAGIC %md
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
