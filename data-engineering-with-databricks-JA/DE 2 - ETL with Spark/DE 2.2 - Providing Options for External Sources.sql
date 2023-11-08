-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-ac3b4f33-4b00-4169-a663-000fddc1fb9d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # 外部ソースに対するオプションの提供
-- MAGIC ファイルへの直接のクエリは、自己記述形式に適していますが、多くのデータソースはレコードを適切に取り込むために追加の構成やスキーマ宣言が必要です。
-- MAGIC
-- MAGIC このレッスンでは、外部データソースを使用してテーブルを作成します。これらのテーブルはまだDelta Lake形式で保存されていないため（したがってLakehouse用に最適化されていないため）、このテクニックは異なる外部システムからデータを抽出するのに役立ちます。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このレッスンの最後までに、以下のことができるようになります：
-- MAGIC - Spark SQLを使用して、外部ソースからデータを抽出するためのオプションを構成する
-- MAGIC - さまざまなファイル形式の外部データソースに対してテーブルを作成する
-- MAGIC - 外部ソースに対して定義されたテーブルをクエリする際のデフォルトの動作を説明する
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-d0bd783f-524d-4953-ac3c-3f1191d42a9e
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## セットアップ実行
-- MAGIC
-- MAGIC セットアップスクリプトを実行すると、データが作成され、このノートブックの残りの部分を実行するために必要な値が宣言されます。
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.2

-- COMMAND ----------

-- DBTITLE 0,--i18n-90830ba2-82d9-413a-974e-97295b7246d0
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## 直接クエリが機能しない場合
-- MAGIC
-- MAGIC CSVファイルは最も一般的なファイル形式の1つですが、これらのファイルに対する直接のクエリはほとんど期待通りの結果を返しません。

-- COMMAND ----------

SELECT * FROM csv.`${DA.paths.sales_csv}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-2de59e8e-3bc3-4609-96ad-e8985b250154
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 上記から次のことがわかります：
-- MAGIC 1. ヘッダー行がテーブル行として抽出されている
-- MAGIC 1. すべての列が1つの列としてロードされている
-- MAGIC 1. ファイルはパイプ区切り（**`|`**）である
-- MAGIC 1. 最終列には切り詰められたネストデータが含まれているようです
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-3eae3be1-134c-4f3b-b423-d081fb780914
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## 外部データに対する読み取りオプションを使用してテーブルを登録
-- MAGIC
-- MAGIC Sparkはデフォルト設定を使用していくつかの自己記述データソースを効率的に抽出しますが、多くの形式はスキーマの宣言やその他のオプションが必要です。
-- MAGIC
-- MAGIC 外部ソースに対してテーブルを作成する際に設定できる<a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">追加の設定</a>が多くありますが、以下の構文はほとんどの形式からデータを抽出するために必要な基本を示しています。
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
-- MAGIC USING data_source<br/>
-- MAGIC OPTIONS (key1 = val1, key2 = val2, ...)<br/>
-- MAGIC LOCATION = path<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC オプションは、キーを引用符で囲まずにテキストで渡し、値は引用符で囲んで渡します。Sparkはカスタムオプションを備えた多くの<a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">データソース</a>をサポートしており、外部<a href="https://docs.databricks.com/libraries/index.html" target="_blank">ライブラリ</a>を介して非公式なサポートを提供しているシステムもあります。
-- MAGIC
-- MAGIC **注意:** ワークスペースの設定に応じて、一部のデータソースに関してはライブラリの読み込みやセキュリティ設定の構成に関する管理者の支援が必要な場合があります。
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-1c947c25-bb8b-4cad-acca-29651e191108
-- MAGIC %md
-- MAGIC
-- MAGIC 以下のセルでは、Spark SQL DDLを使用して、外部のCSVソースに対してテーブルを作成し、以下の情報を指定しています：
-- MAGIC 1. 列名と型
-- MAGIC 1. ファイルフォーマット
-- MAGIC 1. フィールドを区切るために使用される区切り記号
-- MAGIC 1. ヘッダーの存在
-- MAGIC 1. データが格納されているパス

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${DA.paths.sales_csv}"

-- COMMAND ----------

-- DBTITLE 0,--i18n-4631ecfc-06b5-494a-904f-8577e345c98d
-- MAGIC %md
-- MAGIC
-- MAGIC **注意**: PySparkで外部ソースに対してテーブルを作成する場合、このSQLコードを spark.sql() 関数でラップすることができます。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS sales_csv
-- MAGIC   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
-- MAGIC USING CSV
-- MAGIC OPTIONS (
-- MAGIC   header = "true",
-- MAGIC   delimiter = "|"
-- MAGIC )
-- MAGIC LOCATION "{DA.paths.sales_csv}"
-- MAGIC """)

-- COMMAND ----------

-- DBTITLE 0,--i18n-964019da-1d24-4a60-998a-bbf23ffc64a6
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC テーブルの宣言中にデータは移動していないことに注意してください。
-- MAGIC ファイルを直接クエリし、ビューを作成したときと同様に、まだ外部の場所に保存されたファイルを指しているだけです。
-- MAGIC データが正しく読み込まれていることを確認するために、次のセルを実行してください。

-- COMMAND ----------

SELECT * FROM sales_csv

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-d11afb5e-08d3-42c3-8904-14cdddfe5431
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC テーブルの宣言中に渡されたすべてのメタデータとオプションはメタストアに永続化され、その場所のデータは常にこれらのオプションで読み込まれることが保証されます。
-- MAGIC
-- MAGIC **注意:** CSVをデータソースとして使用する際、追加のデータファイルがソースディレクトリに追加される場合、列の順序が変更されないようにすることが重要です。データ形式に強力なスキーマの強制がないため、Sparkはテーブルの宣言中に指定された順序で列をロードし、列名とデータ型を適用します。
-- MAGIC
-- MAGIC テーブルに対して **`DESCRIBE EXTENDED`** を実行すると、テーブル定義に関連付けられたすべてのメタデータが表示されます。
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-fdbb45bc-72b3-4610-97a6-accd30ec8fec
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## 外部データソースを持つテーブルの制限
-- MAGIC
-- MAGIC 外部データソースに対してテーブルやクエリを定義する場合、Delta LakeとLakehouseに関連付けられているパフォーマンス保証を期待できないことに注意してください。 
-- MAGIC 例えば、Delta Lakeテーブルは常にソースデータの最新バージョンをクエリすることを保証しますが、他のデータソースに対して登録されたテーブルは古いキャッシュバージョンを表すことがあります。
-- MAGIC 以下のセルは、テーブルの基になるファイルを直接更新する外部システムを表現するロジックを実行します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC       .option("header", "true")
-- MAGIC       .option("delimiter", "|")
-- MAGIC       .csv(DA.paths.sales_csv)
-- MAGIC       .write.mode("append")
-- MAGIC       .format("csv")
-- MAGIC       .save(DA.paths.sales_csv, header="true"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-28b2112b-4eb2-4bd4-ad76-131e010dfa44
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 現在のテーブル内のレコード数を確認すると、新しく挿入された行は反映されないことに注意してください。

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-bede6aed-2b6b-4ee7-8017-dfce2217e8b4
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 以前にこのデータソースをクエリしたとき、Sparkは自動的に基になるデータをローカルストレージにキャッシュしました。これにより、後続のクエリでは、Sparkはこのローカルキャッシュをクエリするだけで最適なパフォーマンスを提供します。
-- MAGIC
-- MAGIC 外部データソースは、Sparkにデータを更新するように通知するように構成されていません。
-- MAGIC
-- MAGIC データのキャッシュを手動で更新するには、**`REFRESH TABLE`**  コマンドを実行できます。

-- COMMAND ----------

REFRESH TABLE sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-656a9929-a4e5-4fb1-bfe6-6c3cc7137598
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC テーブルをリフレッシュすると、キャッシュが無効になり、元のデータソースを再スキャンし、すべてのデータをメモリに再読み込む必要があります。
-- MAGIC 非常に大規模なデータセットの場合、これにはかなりの時間がかかることがあります。

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-ee1ac9ff-add1-4247-bc44-2e71f0447390
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## SQLデータベースからデータを抽出
-- MAGIC
-- MAGIC SQLデータベースは非常に一般的なデータソースであり、Databricksには多くの種類のSQLに接続する標準のJDBCドライバがあります。
-- MAGIC
-- MAGIC これらの接続を作成する一般的な構文は次のとおりです：
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE <jdbcTable><br/>
-- MAGIC USING JDBC<br/>
-- MAGIC OPTIONS (<br/>
-- MAGIC &nbsp; &nbsp; url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",<br/>
-- MAGIC &nbsp; &nbsp; dbtable = "{jdbcDatabase}.table",<br/>
-- MAGIC &nbsp; &nbsp; user = "{jdbcUsername}",<br/>
-- MAGIC &nbsp; &nbsp; password = "{jdbcPassword}"<br/>
-- MAGIC )
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC 以下のコードサンプルでは、<a href="https://www.sqlite.org/index.html" target="_blank">SQLite</a>と接続します。
-- MAGIC
-- MAGIC **注意:** SQLiteはデータベースを格納するためにローカルファイルを使用し、ポート、ユーザー名、パスワードは必要ありません。
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"> **警告**: JDBCサーバーのバックエンド構成は、このノートブックを単一ノードクラスタで実行していると仮定しています。複数のワーカーを持つクラスタで実行している場合、エグゼキュータで実行されているクライアントはドライバに接続できなくなります。
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:${DA.paths.ecommerce_db}",
  dbtable = "users"
)

-- COMMAND ----------

-- DBTITLE 0,--i18n-33fb962c-707d-43b8-8a37-41ebb5d83b2f
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC これで、このテーブルをローカルに定義されたかのようにクエリできます。

-- COMMAND ----------

SELECT * FROM users_jdbc

-- COMMAND ----------

-- DBTITLE 0,--i18n-3576239e-8f73-4ef9-982e-e42542d4fc70
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC テーブルのメタデータを確認すると、外部システムからスキーマ情報をキャプチャしたことがわかります。
-- MAGIC ストレージプロパティ（接続に関連するユーザー名とパスワードを含む）は自動的にマスキングされます。

-- COMMAND ----------

DESCRIBE EXTENDED users_jdbc

-- COMMAND ----------

-- DBTITLE 0,--i18n-c20051d4-f3c3-483c-b4fa-ff2d356fcd5e
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 指定された場所の内容をリストアップすると、データがローカルに保存されていないことが確認できます。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC
-- MAGIC location = spark.sql("DESCRIBE EXTENDED users_jdbc").filter(F.col("col_name") == "Location").first()["data_type"]
-- MAGIC print(location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(location)
-- MAGIC print(f"Found {len(files)} files")

-- COMMAND ----------

-- DBTITLE 0,--i18n-1cb11f07-755c-4fb2-a122-1eb340033712
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 一部のSQLシステム、たとえばデータウェアハウスなど、独自のドライバを持っています。Sparkはさまざまな外部データベースと異なる方法で対話しますが、基本的なアプローチは次の2つに要約できます：
-- MAGIC 1. ソーステーブル全体をDatabricksに移動し、現在アクティブなクラスタでロジックを実行する
-- MAGIC 1. クエリを外部SQLデータベースにプッシュダウンし、結果のみをDatabricksに転送する
-- MAGIC
-- MAGIC いずれの場合でも、外部SQLデータベースの非常に大規模なデータセットを操作する際には、次のいずれかの理由で大きなオーバーヘッドが発生する可能性があります：
-- MAGIC 1. 全データをパブリックインターネットを介して移動する際のネットワーク転送の遅延
-- MAGIC 1. ビッグデータクエリに最適化されていないソースシステムでのクエリロジックの実行

-- COMMAND ----------

-- DBTITLE 0,--i18n-c973af61-2b79-4c55-8e32-f6a8176ea9e8
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC 以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
