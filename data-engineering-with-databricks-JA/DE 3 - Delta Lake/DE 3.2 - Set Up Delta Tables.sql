-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-c6ad28ff-5ff6-455d-ba62-30880beee5cd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Deltaテーブルの設定
-- MAGIC
-- MAGIC 外部データソースからデータを抽出した後、Databricksプラットフォームの利点を十分に活用できるように、データをLakehouseにロードします。
-- MAGIC
-- MAGIC 異なる組織では、データが最初にDatabricksにロードされる方法についてさまざまなポリシーがあるかもしれませんが、通常、初期のテーブルはデータのほとんど生のバージョンを表すことをお勧めします。検証とエンリッチメントは後の段階で行われるべきです。このパターンにより、データがデータ型や列名に関する期待に一致しない場合でも、データが削除されないため、プログラムまたは手動の介入によって部分的に壊れたまたは無効な状態のデータを救うことができます。
-- MAGIC
-- MAGIC このレッスンでは、ほとんどのテーブルを作成するために使用されるパターン、**`CREATE TABLE _ AS SELECT`** (CTAS) ステートメントに焦点を当てます。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このレッスンの最後までに、次のことができるようになるはずです：
-- MAGIC - CTASステートメントを使用してDelta Lakeテーブルを作成する
-- MAGIC - 既存のビューまたはテーブルから新しいテーブルを作成する
-- MAGIC - ロードされたデータに追加のメタデータを付加する
-- MAGIC - 生成された列と記述的なコメントを持つテーブルスキーマを宣言する
-- MAGIC - データの場所、品質の強制、およびパーティショニングを制御するための高度なオプションを設定する
-- MAGIC - シャロークローンとディープクローンを作成する
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-fe1b3b29-37fb-4e5b-8a50-e2baf7381d24
-- MAGIC %md
-- MAGIC
-- MAGIC ## セットアップの実行
-- MAGIC
-- MAGIC セットアップスクリプトはデータを作成し、このノートブックの残りの部分が実行できるようにするために必要な値を宣言します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.4

-- COMMAND ----------

-- DBTITLE 0,--i18n-36f64593-7d14-4841-8b1a-fabad267ba22
-- MAGIC %md
-- MAGIC
-- MAGIC ## SELECTを使用したテーブルの作成（CTAS）
-- MAGIC
-- MAGIC **`CREATE TABLE AS SELECT`** ステートメントは、入力クエリから取得したデータを使用してDeltaテーブルを作成し、データを追加します。
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${DA.paths.datasets}/ecommerce/raw/sales-historical`;

DESCRIBE EXTENDED sales;

-- COMMAND ----------

-- DBTITLE 0,--i18n-1d3d7f45-be4f-4459-92be-601e55ff0063
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC CTASステートメントは、クエリの結果からスキーマ情報を自動的に推論し、手動でスキーマを宣言することは**サポートされていません**。
-- MAGIC
-- MAGIC これは、CTASステートメントがスキーマが明示的に定義されたソースからの外部データ取り込みに適していることを意味します。例えば、Parquetファイルやテーブルなどです。
-- MAGIC
-- MAGIC CTASステートメントはまた、追加のファイルオプションを指定することもサポートしていません。
-- MAGIC
-- MAGIC CSVファイルからデータを取り込む際に、これが大きな制約となることが想像できます。

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${da.paths.datasets}/ecommerce/raw/sales-csv`;

SELECT * FROM sales_unparsed;

-- COMMAND ----------

-- DBTITLE 0,--i18n-0ec18c0e-c56b-4b41-8364-4826d1f34dcf
-- MAGIC %md
-- MAGIC
-- MAGIC このデータを正しくDelta Lakeテーブルに取り込むために、オプションを指定できるファイルへの参照を使用する必要があります。
-- MAGIC
-- MAGIC 前のレッスンでは、これを外部テーブルを登録することで行う方法を示しました。ここでは、この構文をわずかに進化させて、オプションを一時ビューに指定し、その後この一時ビューをCTASステートメントのソースとして使用してDeltaテーブルを正常に登録します。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/ecommerce/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta

-- COMMAND ----------

-- DBTITLE 0,--i18n-96f21158-ccb9-4fd3-9dd2-c7c7fce1a6e9
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## 既存のテーブルから列をフィルタリングおよび名前変更
-- MAGIC
-- MAGIC 列の名前を変更したり、対象のテーブルから列を省略したりするような簡単な変換は、テーブルの作成中に簡単に行うことができます。
-- MAGIC
-- MAGIC 次のステートメントは、 **`sales`** テーブルからの列のサブセットを含む新しいテーブルを作成します。
-- MAGIC
-- MAGIC ここでは、意図的にユーザーを特定する可能性のある情報や個別の購入詳細を提供する情報を省略していると仮定します。また、下流のシステムがソースデータと異なる命名規則を持っていると仮定し、フィールド名を変更します。

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases

-- COMMAND ----------

-- DBTITLE 0,--i18n-02026f25-a1cf-42e5-b9c3-75b9c1c7ef11
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 同じ目標をビューを使用して達成することもできることに注意してください。以下に示すように。

-- COMMAND ----------

CREATE OR REPLACE VIEW purchases_vw AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases_vw

-- COMMAND ----------

-- DBTITLE 0,--i18n-23f77f7e-21d5-4977-93bc-45800b28535f
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## 生成された列を使用したスキーマの宣言
-- MAGIC
-- MAGIC 前述のように、CTASステートメントはスキーマの宣言をサポートしていません。上記で、タイムスタンプ列はUnixタイムスタンプのバリアントのようであり、アナリストが洞察を得るのに最も役立つ情報ではないようです。これは、生成された列が役立つ状況です。
-- MAGIC
-- MAGIC 生成された列は、Deltaテーブル内の他の列に基づいて自動的に生成される値を持つ特別なタイプの列です（DBR 8.3で導入）。
-- MAGIC
-- MAGIC 以下のコードは、新しいテーブルを作成しながら以下の操作を行います：
-- MAGIC 1. 列名と型を指定する
-- MAGIC 1. <a href="https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns" target="_blank">生成された列</a>を追加して日付を計算する
-- MAGIC 1. 生成された列の説明的な列コメントを提供する

-- COMMAND ----------

CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")

-- COMMAND ----------

-- DBTITLE 0,--i18n-33e94ae0-f443-4cc9-9691-30b8b08179aa
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC
-- MAGIC **`date`** は生成された列であるため、 **`date`** 列の値を提供せずに **`purchase_dates`** に書き込む場合、Delta Lakeは自動的に計算します。
-- MAGIC
-- MAGIC **注意**: 以下のセルでは、Delta Lake **`MERGE`** ステートメントを使用する際に列を生成できるように設定を行っています。この構文については、コースの後半で詳しく説明します。

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

-- DBTITLE 0,--i18n-8da2bdf3-a0e1-4c5d-a016-d9bc38167f50
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC 以下から、データが挿入されるにつれてすべての日付が正しく計算されたことがわかります。ただし、ソースデータや挿入クエリはこのフィールドの値を指定していません。
-- MAGIC
-- MAGIC Delta Lakeソースの場合、クエリは常にクエリのためのテーブルの最新のスナップショットを自動的に読み取ります。したがって、 **`REFRESH TABLE`** を実行する必要はありません。

-- COMMAND ----------

SELECT * FROM purchase_dates

-- COMMAND ----------

-- DBTITLE 0,--i18n-3bb038ec-4e33-40a1-b8ff-33b388e5dda1
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC 重要なことは、生成されたフィールドであるはずのフィールドがテーブルへの挿入に含まれている場合、提供された値が生成された列を定義するロジックで導出される値と完全に一致しない場合、この挿入は失敗することです。
-- MAGIC
-- MAGIC 以下のセルをコメント解除して実行することで、このエラーを確認できます。

-- COMMAND ----------

-- INSERT INTO purchase_dates VALUES
-- (1, 600000000, 42.0, "2020-06-18")

-- COMMAND ----------

-- DBTITLE 0,--i18n-43e1ab8b-0b34-4693-9d49-29d1f899c210
-- MAGIC %md
-- MAGIC
-- MAGIC ## テーブル制約の追加
-- MAGIC
-- MAGIC 上記のエラーメッセージは、 **`CHECK制約`** を指しています。生成された列は、チェック制約の特別な実装です。
-- MAGIC
-- MAGIC Delta Lakeはスキーマの書き込み時に強制するため、Databricksはテーブルに追加されるデータの品質と整合性を確保するための標準SQL制約管理節をサポートできます。
-- MAGIC
-- MAGIC Databricksは現在、2つのタイプの制約をサポートしています:
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">**`NOT NULL`** 制約</a>
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">**`CHECK`** 制約</a>
-- MAGIC
-- MAGIC どちらの場合も、制約を定義する前に、制約に違反するデータが既にテーブルに存在しないことを確認する必要があります。一度制約がテーブルに追加されると、制約に違反するデータは書き込みエラーを引き起こします。
-- MAGIC
-- MAGIC 以下では、テーブルの **`date`** 列に **`CHECK`** 制約を追加します。 **`CHECK`** 制約は、データセットをフィルタリングするために使用するかもしれない標準の **`WHERE`** 句のように見えます。

-- COMMAND ----------

ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

-- COMMAND ----------

-- DBTITLE 0,--i18n-32fe077c-4e4d-4830-9a80-9a6a2b5d2a61
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC テーブル制約は、 **`TBLPROPERTIES`** フィールドに表示されます。

-- COMMAND ----------

DESCRIBE EXTENDED purchase_dates

-- COMMAND ----------

-- DBTITLE 0,--i18n-07f549c0-71af-4271-a8f5-91b4237d89e4
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## 付加的なオプションとメタデータを使用してテーブルを拡張
-- MAGIC
-- MAGIC これまで、Delta Lake テーブルを拡張するためのオプションについてほんの一部しか触れていません。
-- MAGIC
-- MAGIC 以下では、CTAS ステートメントを進化させ、さまざまな追加の構成とメタデータを含める方法を示します。
-- MAGIC
-- MAGIC **`SELECT`** 句では、ファイルの取り込みに役立つ2つの組み込み Spark SQL コマンドを活用しています：
-- MAGIC * **`current_timestamp()`** は、ロジックが実行されたタイムスタンプを記録します
-- MAGIC * **`input_file_name()`** は、テーブル内の各レコードのソースデータファイルを記録します
-- MAGIC
-- MAGIC また、ソースのタイムスタンプデータから派生した新しい日付列を作成するロジックも含まれています。
-- MAGIC
-- MAGIC **`CREATE TABLE`** 句にはいくつかのオプションが含まれています：
-- MAGIC * テーブルの内容を簡単に見つけるために **`COMMENT`** が追加されています
-- MAGIC * **`LOCATION`** が指定されており、管理されていない（管理されていないではなく、外部の）テーブルになります
-- MAGIC * テーブルは日付列によって **`PARTITIONED BY`** されており、各日付のデータが対象のストレージ場所内の独自のディレクトリに存在することを意味します
-- MAGIC
-- MAGIC **注意**: パーティショニングは、主に構文と影響を示すためにここに示されています。ほとんどの Delta Lake テーブル（特に小～中規模のデータ）はパーティショニングの恩恵を受けません。パーティショニングはデータファイルを物理的に分離するため、小さなファイルの問題を引き起こし、ファイルの圧縮と効率的なデータのスキップを妨げる可能性があります。Hive または HDFS で観察されるメリットは Delta Lake には適用されず、テーブルをパーティション分割する前に経験豊富な Delta Lake アーキテクトと相談する必要があります。
-- MAGIC
-- MAGIC **ほとんどのユースケースでは、Delta Lake で作業する際には非パーティションテーブルをデフォルトで選択すべきです。**

-- COMMAND ----------

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-historical/`;
  
SELECT * FROM users_pii;

-- COMMAND ----------

-- DBTITLE 0,--i18n-431d473d-162f-4a97-9afc-df47a787f409
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC テーブルに追加されたメタデータフィールドは、レコードがいつ挿入されたか、どこから来たかを理解するのに役立つ情報を提供します。これは、ソースデータの問題をトラブルシューティングする必要がある場合に特に役立ちます。
-- MAGIC
-- MAGIC 与えられたテーブルのすべてのコメントとプロパティは、 **`DESCRIBE TABLE EXTENDED`** を使用して確認できます。
-- MAGIC
-- MAGIC **注意**: Delta Lakeは、テーブル作成時に自動的にいくつかのテーブルプロパティを追加します。

-- COMMAND ----------

DESCRIBE EXTENDED users_pii

-- COMMAND ----------

-- DBTITLE 0,--i18n-227fec69-ab44-47b4-aef3-97a14eb4384a
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC テーブルに使用される場所をリストすると、パーティションカラム **`first_touch_date`** のユニークな値がデータディレクトリを作成するために使用されていることがわかります。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.working_dir}/tmp/users_pii")
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-d188161b-3bc6-4095-aec9-508c09c14e0c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Delta Lakeテーブルのクローン
-- MAGIC Delta LakeにはDelta Lakeテーブルを効率的にコピーするための2つのオプションがあります。
-- MAGIC
-- MAGIC **`DEEP CLONE`** は、データとメタデータをソーステーブルからターゲットに完全にコピーします。
-- MAGIC このコピーは段階的に行われるため、このコマンドを再実行すると、ソースからターゲットの場所への変更が同期されます。
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases

-- COMMAND ----------

-- DBTITLE 0,--i18n-c0aa62a8-7448-425c-b9de-45284ea87f8c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC すべてのデータファイルをコピーする必要があるため、大規模なデータセットにはかなりの時間がかかることがあります。
-- MAGIC
-- MAGIC 現在のテーブルを変更せずに変更を適用してテストするためにテーブルのコピーを迅速に作成したい場合、 **`SHALLOW CLONE`** は良いオプションです。
-- MAGIC シャロークローンはDeltaトランザクションログだけをコピーするため、データは移動しません。
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases

-- COMMAND ----------

-- DBTITLE 0,--i18n-045bfc09-41cc-4710-ab67-acab3881f128
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC どちらの場合でも、テーブルのクローンに適用されるデータの変更は、ソースから別個に追跡および保存されます。
-- MAGIC クローンは、開発中のSQLコードのテスト用にテーブルをセットアップする素晴らしい方法です。

-- COMMAND ----------

-- DBTITLE 0,--i18n-32c272d6-cbe8-43ba-b051-9fe8e5586990
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## サマリー
-- MAGIC
-- MAGIC このノートブックでは、主にDelta Lakeテーブルを作成するためのDDLと構文に焦点を当てました。次のノートブックでは、テーブルへの更新の書き込みオプションを探索します。

-- COMMAND ----------

-- DBTITLE 0,--i18n-c87f570b-e175-4000-8706-c571aa1cf6e1
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
