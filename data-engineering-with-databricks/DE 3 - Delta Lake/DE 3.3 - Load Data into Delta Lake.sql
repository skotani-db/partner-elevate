-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-a518bafd-59bd-4b23-95ee-5de2344023e4
-- MAGIC %md
-- MAGIC
-- MAGIC # Delta Lakeへのデータの読み込み
-- MAGIC Delta Lakeテーブルは、クラウドオブジェクトストレージでバックアップされたデータファイルを持つテーブルにACID準拠の更新を提供します。
-- MAGIC
-- MAGIC このノートブックでは、Delta Lakeで更新を処理するためのSQL構文を探求します。多くの操作は標準のSQLですが、SparkとDelta Lakeの実行に合わせてわずかな変更があります。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このレッスンの最後までに、次のことができるようになるはずです：
-- MAGIC - **`INSERT OVERWRITE`** を使用してデータテーブルを上書きする
-- MAGIC - **`INSERT INTO`** を使用してテーブルに追加する
-- MAGIC - **`MERGE INTO`** を使用してテーブルに追加、更新、削除を行う
-- MAGIC - **`COPY INTO`** を使用してテーブルにデータを増分的に読み込む
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-af486892-a86c-4ef2-9996-2ace24b5737c
-- MAGIC %md
-- MAGIC
-- MAGIC ## セットアップの実行
-- MAGIC
-- MAGIC セットアップスクリプトはデータを作成し、このノートブックの残りの部分が実行できるようにするために必要な値を宣言します。
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.5

-- COMMAND ----------

-- DBTITLE 0,--i18n-04a35896-fb09-4a99-8d00-313480e5c6a1
-- MAGIC %md
-- MAGIC
-- MAGIC ## 完全な上書き
-- MAGIC
-- MAGIC テーブル内のすべてのデータを原子的に置き換えるために上書きを使用できます。テーブルを削除して再作成する代わりにテーブルを上書きすることには、複数の利点があります：
-- MAGIC - テーブルを上書きする方がはるかに高速です。ディレクトリを再帰的にリストアップしたり、ファイルを削除したりする必要がないためです。
-- MAGIC - テーブルの古いバージョンはまだ存在し、タイムトラベルを使用して簡単に古いデータを取得できます。
-- MAGIC - これは原子操作です。テーブルを削除している間でも、同時クエリはテーブルを読み取ることができます。
-- MAGIC - ACIDトランザクションの保証により、テーブルの上書きに失敗した場合、テーブルは以前の状態になります。
-- MAGIC
-- MAGIC Spark SQLは完全な上書きを実行するための2つの簡単な方法を提供しています。
-- MAGIC
-- MAGIC 前のレッスンでCTASステートメントについて学んだ学生の中には、実際にはCRASステートメントを使用したことに気付いたかもしれません（セルが複数回実行された場合の潜在的なエラーを回避するためです）。
-- MAGIC
-- MAGIC **`CREATE OR REPLACE TABLE`** （CRAS）ステートメントは、実行ごとにテーブルの内容を完全に置き換えます。
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE events AS
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/events-historical`

-- COMMAND ----------

-- DBTITLE 0,--i18n-8f767697-33e6-4b5b-ac09-862076f77033
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC テーブルの履歴を確認すると、以前のバージョンのこのテーブルが置き換えられたことがわかります。

-- COMMAND ----------

DESCRIBE HISTORY events

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb68d513-240c-41e1-902c-3c3add9c0a75
-- MAGIC %md
-- MAGIC
-- MAGIC **`INSERT OVERWRITE`** は、上記とほぼ同じ結果を提供します：ターゲットテーブル内のデータはクエリからのデータで置き換えられます。
-- MAGIC
-- MAGIC **`INSERT OVERWRITE`**：
-- MAGIC
-- MAGIC - 既存のテーブルを上書きすることしかできず、CRASステートメントのように新しいテーブルを作成することはできません
-- MAGIC - 現在のテーブルスキーマに一致する新しいレコードでのみ上書きでき、したがって、下流の消費者を中断せずに既存のテーブルを上書きする「安全な」テクニックとなります
-- MAGIC - 個々のパーティションを上書きできます
-- MAGIC

-- COMMAND ----------

INSERT OVERWRITE sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical/`

-- COMMAND ----------

-- DBTITLE 0,--i18n-cfefb85f-f762-43db-be9b-cb536a06c842
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC CRASステートメントとは異なるメトリクスが表示されることに注意してください。また、テーブルの履歴も操作を異なる方法で記録します。

-- COMMAND ----------

DESCRIBE HISTORY sales

-- COMMAND ----------

-- DBTITLE 0,--i18n-40769b04-c72b-4740-9d27-ea2d1b8700f3
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ここでの主な違いは、Delta Lakeが書き込み時のスキーマをどのように強制するかに関係しています。
-- MAGIC
-- MAGIC CRASステートメントはターゲットテーブルの内容を完全に再定義することを許可しますが、**`INSERT OVERWRITE`** はスキーマを変更しようとすると失敗します（オプションの設定を提供しない限り）。
-- MAGIC
-- MAGIC 以下のセルのコメントを解除して実行して、期待されるエラーメッセージを生成してください。

-- COMMAND ----------

-- INSERT OVERWRITE sales
-- SELECT *, current_timestamp() FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical`

-- COMMAND ----------

-- DBTITLE 0,--i18n-ceb78e46-6362-4c3b-b63d-54f42d38dd1f
-- MAGIC %md
-- MAGIC
-- MAGIC ## 行の追加
-- MAGIC
-- MAGIC **`INSERT INTO`** を使用して、既存のDeltaテーブルに新しい行を原子的に追加できます。これにより、毎回上書きするよりも効率的な既存のテーブルへの増分更新が可能になります。
-- MAGIC
-- MAGIC **`INSERT INTO`** を使用して、**`sales`** テーブルに新しい販売レコードを追加します。
-- MAGIC

-- COMMAND ----------

INSERT INTO sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-30m`

-- COMMAND ----------

-- DBTITLE 0,--i18n-171f9cf2-e0e5-4f8d-9dc7-bf4770b6d8e5
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC **`INSERT INTO`** には、同じレコードを複数回挿入するのを防ぐための組み込みの保証がないことに注意してください。上記のセルを再実行すると、同じレコードがターゲットテーブルに書き込まれ、重複したレコードが生成されます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-5ad4ab1f-a7c1-439d-852e-ff504dd16307
-- MAGIC %md
-- MAGIC
-- MAGIC ## マージ更新
-- MAGIC
-- MAGIC **`MERGE`** SQL操作を使用して、ソーステーブル、ビュー、またはDataFrameからターゲットDeltaテーブルにデータをアップサートできます。 Delta Lakeは **`MERGE`** での挿入、更新、削除をサポートし、高度なユースケースを簡素化するためのSQL標準を超えた拡張構文をサポートしています。
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC MERGE INTO target a<br/>
-- MAGIC USING source b<br/>
-- MAGIC ON {merge_condition}<br/>
-- MAGIC WHEN MATCHED THEN {matched_action}<br/>
-- MAGIC WHEN NOT MATCHED THEN {not_matched_action}<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC 私たちは**`MERGE`**操作を使用して、更新されたメールアドレスと新しいユーザーを持つ過去のユーザーデータを更新します。
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_update AS 
SELECT *, current_timestamp() AS updated 
FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-30m`

-- COMMAND ----------

-- DBTITLE 0,--i18n-4732ea19-2857-45fe-9ca2-c2475015ef47
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC **`MERGE`** の主な利点：
-- MAGIC * 更新、挿入、削除が1つのトランザクションとして完了します
-- MAGIC * マッチングフィールドに加えて複数の条件を追加できます
-- MAGIC * カスタムロジックを実装するための幅広いオプションを提供します
-- MAGIC
-- MAGIC 以下では、現在の行の**`NULL`**のメールアドレスと新しい行がメールアドレスを持っている場合にのみレコードを更新します。
-- MAGIC
-- MAGIC 新しいバッチのすべてのマッチしないレコードは挿入されます。

-- COMMAND ----------

MERGE INTO users a
USING users_update b
ON a.user_id = b.user_id
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
  UPDATE SET email = b.email, updated = b.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- DBTITLE 0,--i18n-5cae1734-7eaf-4a53-a9b5-c093a8d73cc9
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC この関数の動作を **`MATCHED`** および **`NOT MATCHED`** の両条件に対して明示的に指定しています。ここで示されている例は適用できるロジックの一例であり、 **`MERGE`** のすべての動作を示すものではありません。

-- COMMAND ----------

-- DBTITLE 0,--i18n-d7d2c7fd-2c83-4ed2-aa78-c37992751881
-- MAGIC %md
-- MAGIC
-- MAGIC ## デデュプリケーションのためのインサート専用マージ
-- MAGIC
-- MAGIC 一般的なETLユースケースは、ログや他の絶えず増加するデータセットを、一連の追加操作を通じてDeltaテーブルに収集することです。
-- MAGIC
-- MAGIC 多くのソースシステムは重複レコードを生成することがあります。マージを使用すると、デュプリケートレコードを挿入せずに、インサート専用のマージを実行して回避できます。
-- MAGIC
-- MAGIC この最適化されたコマンドは同じ **`MERGE`** 構文を使用しますが、**`WHEN NOT MATCHED`** 句のみを指定します。
-- MAGIC
-- MAGIC 以下では、同じ **`user_id`** と **`event_timestamp`** を持つレコードがすでに **`events`** テーブルに存在しないことを確認するためにこれを使用します。
-- MAGIC

-- COMMAND ----------

MERGE INTO events a
USING events_update b
ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
WHEN NOT MATCHED AND b.traffic_source = 'email' THEN 
  INSERT *

-- COMMAND ----------

-- DBTITLE 0,--i18n-75891a95-c6f2-4f00-b30e-3df2df858c7c
-- MAGIC %md
-- MAGIC
-- MAGIC ## 増分的な読み込み
-- MAGIC
-- MAGIC **`COPY INTO`** はSQLエンジニアに対して外部システムからデータを増分的に読み込むための冪等性のあるオプションを提供します。
-- MAGIC
-- MAGIC この操作にはいくつかの期待があることに注意してください：
-- MAGIC - データスキーマは一貫している必要があります
-- MAGIC - 重複するレコードは排除または下流で処理する必要があります
-- MAGIC
-- MAGIC この操作は、予測可能に成長するデータに対して完全なテーブルスキャンよりもはるかに安価かもしれません。
-- MAGIC
-- MAGIC ここでは静的なディレクトリでの単純な実行を示しますが、実際の価値は時間をかけて複数回実行し、ソースから新しいファイルを自動的に収集することにあります。
-- MAGIC

-- COMMAND ----------

COPY INTO sales
FROM "${da.paths.datasets}/ecommerce/raw/sales-30m"
FILEFORMAT = PARQUET

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd65fe71-cdaf-47a8-85ec-fa9769c11708
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC 以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
