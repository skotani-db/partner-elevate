-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-0d527322-1a21-4a91-bc34-e7957e052a75
-- MAGIC %md
-- MAGIC
-- MAGIC # Delta Lakeにおけるバージョニング、最適化、Vacuuming
-- MAGIC
-- MAGIC Delta Lakeで基本的なデータタスクを実行するのに慣れたら、Delta Lake固有のいくつかの機能について話し合うことができます。
-- MAGIC
-- MAGIC ここで使用されているキーワードの一部は、標準のANSI SQLの一部ではないことに注意してください。ただし、すべてのDelta Lake操作はDatabricksを使用してSQLで実行できます。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このレッスンの最後までに、次のことができるようになるはずです：
-- MAGIC * **`OPTIMIZE`** を使用して小さなファイルを圧縮する
-- MAGIC * **`ZORDER`** を使用してテーブルをインデックス化する
-- MAGIC * Delta Lakeファイルのディレクトリ構造を説明する
-- MAGIC * テーブルトランザクションの履歴を確認する
-- MAGIC * 前のテーブルバージョンにクエリを実行し、ロールバックする
-- MAGIC * **`VACUUM`** を使用して古いデータファイルを削除する
-- MAGIC
-- MAGIC **リソース**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Databricks Docs</a>

-- COMMAND ----------

-- DBTITLE 0,--i18n-ef1115dd-7242-476a-a929-a16aa09ce9c1
-- MAGIC %md
-- MAGIC
-- MAGIC ## セットアップ実行
-- MAGIC 最初に実行することは、セットアップスクリプトを実行することです。これにより、ユーザごとにスコープが設定されたユーザ名、ユーザホーム、およびスキーマが定義されます。
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.2 

-- COMMAND ----------

-- DBTITLE 0,--i18n-b10dbe8f-e936-4ca3-9d1e-8b471c4bc162
-- MAGIC %md
-- MAGIC
-- MAGIC ## ヒストリーを持つDeltaテーブルの作成
-- MAGIC
-- MAGIC このクエリの実行を待っている間、実行されているトランザクションの合計数を特定できるか試してみてください。
-- MAGIC

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

-- DBTITLE 0,--i18n-e932d675-aa26-42a7-9b55-654ac9896dab
-- MAGIC %md
-- MAGIC
-- MAGIC ## テーブルの詳細を調べる
-- MAGIC
-- MAGIC デフォルトでDatabricksはHiveメタストアを使用してスキーマ、テーブル、およびビューを登録します。
-- MAGIC
-- MAGIC **`DESCRIBE EXTENDED`** を使用すると、テーブルに関する重要なメタデータを表示できます。
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- DBTITLE 0,--i18n-a6be5873-30b3-4e7e-9333-2c1e6f1cbe25
-- MAGIC %md
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`** は、テーブルのメタデータを探索するための別のコマンドです。
-- MAGIC

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd7b24fa-7a2d-4f31-ab7c-fcc28d617d75
-- MAGIC %md
-- MAGIC
-- MAGIC **`Location`** フィールドに注目してください。
-- MAGIC
-- MAGIC これまで、私たちはテーブルを単なるスキーマ内の関係的なエンティティとして考えていましたが、Delta Lakeテーブルは実際にはクラウドオブジェクトストレージに格納されたファイルのコレクションでサポートされています。
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-0ff9d64a-f0c4-4ee6-a007-888d4d082abe
-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Lakeファイルを探索する
-- MAGIC
-- MAGIC Databricksのユーティリティ関数を使用して、Delta Lakeテーブルをサポートするファイルを見ることができます。
-- MAGIC
-- MAGIC **注意**: Delta Lakeを操作するためにこれらのファイルのすべてを知る必要はありませんが、技術がどのように実装されているかを理解するのに役立ちます。
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-1a84bb11-649d-463b-85ed-0125dc599524
-- MAGIC %md
-- MAGIC
-- MAGIC ディレクトリにはいくつかのParquetデータファイルと、 **`_delta_log`** という名前のディレクトリが含まれていることに注意してください。
-- MAGIC
-- MAGIC Delta Lakeテーブルのレコードは、Parquetファイル内のデータとして保存されます。
-- MAGIC
-- MAGIC Delta Lakeテーブルへのトランザクションは、 **`_delta_log`** に記録されます。
-- MAGIC
-- MAGIC **`_delta_log`** 内部をのぞいて詳細を確認できます。
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-dbcbd76a-c740-40be-8893-70e37bd5e0d2
-- MAGIC %md
-- MAGIC
-- MAGIC 各トランザクションは、Delta Lakeトランザクションログに新しいJSONファイルが書き込まれる結果としています。ここでは、このテーブルに対して合計8つのトランザクションがあることがわかります（Delta Lakeは0から始まるインデックスです）。
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-101dffc0-260a-4078-97db-cb1de8d705a8
-- MAGIC %md
-- MAGIC
-- MAGIC ## データファイルについての考察
-- MAGIC
-- MAGIC 明らかに非常に小さなテーブルに対して多くのデータファイルを見ました。
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`** を使用すると、デルタテーブルに関する他の詳細を見ることができます。ファイルの数も含まれています。
-- MAGIC

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- DBTITLE 0,--i18n-cb630727-afad-4dde-9d71-bcda9e579de9
-- MAGIC %md
-- MAGIC
-- MAGIC ここでは、現在のバージョンのテーブルには4つのデータファイルが含まれていることがわかります。では、テーブルディレクトリにあるその他のParquetファイルは何をしているのでしょうか？
-- MAGIC
-- MAGIC Delta Lakeは、変更されたデータを含むファイルを直ちに上書きまたは削除せず、トランザクションログを使用してファイルがテーブルの現在のバージョンで有効かどうかを示します。
-- MAGIC
-- MAGIC ここでは、上記の **`MERGE`** ステートメントに対応するトランザクションログを見てみます。ここで、レコードが挿入、更新、削除されました。
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-3221b77b-6d57-4654-afc3-dcb9dfa62be8
-- MAGIC %md
-- MAGIC
-- MAGIC **`add`** 列には、テーブルに書き込まれたすべての新しいファイルのリストが含まれており、**`remove`** 列はもはやテーブルに含まれていてはいけないファイルを示しています。
-- MAGIC
-- MAGIC Delta Lakeテーブルをクエリするとき、クエリエンジンはトランザクションログを使用して、現在のバージョンで有効なすべてのファイルを解決し、他のすべてのデータファイルを無視します。
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-bc6dee2e-406c-48b2-9780-74408c93162d
-- MAGIC %md
-- MAGIC
-- MAGIC ## 小さなファイルの圧縮とインデックスの作成
-- MAGIC
-- MAGIC 小さなファイルはさまざまな理由で発生する可能性があります。私たちの場合、1つまたは数レコードだけが挿入された操作をいくつか実行しました。
-- MAGIC
-- MAGIC ファイルは、**`OPTIMIZE`** コマンドを使用して最適なサイズに結合されます（テーブルのサイズに基づいてスケーリングされます）。
-- MAGIC
-- MAGIC **`OPTIMIZE`** は、既存のデータファイルを置き換え、レコードを結合して結果を書き直します。
-- MAGIC
-- MAGIC **`OPTIMIZE`** を実行する際、ユーザーは任意で**`ZORDER`** インデックスのための1つまたは複数のフィールドを指定できます。Z-orderの具体的な数学は重要ではありませんが、提供されたフィールドでフィルタリングを行う際にデータを同じ値を持つデータファイル内に配置することでデータの取得を高速化します。
-- MAGIC

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

-- DBTITLE 0,--i18n-5f412c12-88c7-4e43-bda2-60ec5c749b2a
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Given how small our data is, **`ZORDER`** does not provide any benefit, but we can see all of the metrics that result from this operation.

-- COMMAND ----------

-- DBTITLE 0,--i18n-2ad93f7e-4bb1-4051-8b9c-b685164e3b45
-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Lakeトランザクションの確認
-- MAGIC
-- MAGIC Delta Lakeテーブルへのすべての変更はトランザクションログに保存されているため、簡単に<a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">テーブルの履歴</a>を確認できます。
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

-- DBTITLE 0,--i18n-ed297545-7997-4e75-8bf6-0c204a707956
-- MAGIC %md
-- MAGIC
-- MAGIC 予想通り、**`OPTIMIZE`** は私たちのテーブルの別のバージョンを作成し、バージョン8が最新のバージョンであることを意味します。
-- MAGIC
-- MAGIC トランザクションログで削除されたとマークされた余分なデータファイルを覚えていますか？これらは、テーブルの以前のバージョンをクエリする機能を提供しています。
-- MAGIC
-- MAGIC これらのタイムトラベルクエリは、整数のバージョンまたはタイムスタンプを指定して実行できます。
-- MAGIC
-- MAGIC **注意**: ほとんどの場合、興味のある時点でデータを再作成するためにタイムスタンプを使用します。デモではバージョンを使用しますが、これは確定的です（将来の任意のタイミングでデモを実行する可能性があるため）。
-- MAGIC

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3

-- COMMAND ----------

-- DBTITLE 0,--i18n-d1d03156-6d88-4d4c-ae8e-ddfe49d957d7
-- MAGIC %md
-- MAGIC
-- MAGIC タイムトラベルについて注意すべき重要な点は、現在のバージョンに対するトランザクションを元に戻して以前のテーブルの状態を再作成しているわけではなく、指定したバージョンで有効とされているすべてのデータファイルをクエリしているだけだということです。
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-78cf75b0-0403-4aa5-98c7-e3aabbef5d67
-- MAGIC %md
-- MAGIC
-- MAGIC ## バージョンをロールバックする
-- MAGIC
-- MAGIC テーブルから一部のレコードを手動で削除するクエリを入力しようとして、次の状態でこのクエリを誤って実行したと仮定しましょう。
-- MAGIC

-- COMMAND ----------

DELETE FROM students

-- COMMAND ----------

-- DBTITLE 0,--i18n-7f7936c3-3aa2-4782-8425-78e6e7634d79
-- MAGIC %md
-- MAGIC
-- MAGIC データ削除によって影響を受けた行数が**`-1`**と表示される場合、データのディレクトリ全体が削除されたことを意味します。
-- MAGIC
-- MAGIC これを以下で確認しましょう。
-- MAGIC

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- DBTITLE 0,--i18n-9d3908f4-e6bb-40a6-92dd-d7d12d28a032
-- MAGIC %md
-- MAGIC
-- MAGIC テーブルのすべてのレコードを削除することはおそらく望ましくありません。幸い、このコミットを簡単にロールバックできます。
-- MAGIC

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8 

-- COMMAND ----------

-- DBTITLE 0,--i18n-902966c3-830a-44db-9e59-dec82b98a9c2
-- MAGIC %md
-- MAGIC
-- MAGIC **`RESTORE`** <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">コマンド</a> はトランザクションとして記録されます。テーブルのすべてのレコードを誤って削除した事実を完全に隠すことはできませんが、操作を元に戻し、テーブルを望ましい状態に戻すことはできます。
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-847452e6-2668-463b-afdf-52c1f512b8d3
-- MAGIC %md
-- MAGIC
-- MAGIC ## 古いファイルをクリーンアップする
-- MAGIC
-- MAGIC DatabricksはDelta Lakeテーブル内の古いログファイル（デフォルトでは30日以上）を自動的にクリーンアップします。
-- MAGIC チェックポイントが書き込まれるたびに、Databricksはこの保持期間よりも古いログエントリを自動的にクリーンアップします。
-- MAGIC
-- MAGIC Delta Lakeのバージョニングとタイムトラベルは、最新バージョンのクエリとクエリのロールバックには適していますが、大規模な本番テーブルのすべてのバージョンのデータファイルを無期限に保持することは非常に高価です（また、PIIが存在する場合、コンプライアンスの問題につながる可能性があります）。
-- MAGIC
-- MAGIC 古いデータファイルを手動で削除する場合、これは **`VACUUM`** 操作で実行できます。
-- MAGIC
-- MAGIC 次のセルのコメントを解除し、 **`0 HOURS`** の保持期間で実行して、現在のバージョンのみを保持できます：
-- MAGIC

-- COMMAND ----------

-- VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-b3af389e-e93f-433c-8d47-b38f8ded5ecd
-- MAGIC %md
-- MAGIC
-- MAGIC デフォルトでは、**`VACUUM`** は7日未満のファイルを削除しないようにします。これは、削除対象のファイルを参照している長時間実行される操作がないことを確認するためです。Deltaテーブルで**`VACUUM`** を実行すると、指定されたデータ保持期間よりも古いバージョンにタイムトラベルする能力が失われます。デモでは、**`0 HOURS`** の保持を指定するコードを実行する場面があるかもしれません。これは単にその機能をデモするためであり、通常は本番環境では行いません。
-- MAGIC
-- MAGIC 次のセルでは、次の操作を行います：
-- MAGIC 1. データファイルの早期削除を防ぐチェックを無効にする
-- MAGIC 2. **`VACUUM`** コマンドのログ記録を有効にする
-- MAGIC 3. ドライランバージョンの **`VACUUM`** を使用して削除されるすべてのレコードを表示します
-- MAGIC

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- DBTITLE 0,--i18n-7c825ee6-e584-48a1-8d75-f616d7ed53ac
-- MAGIC %md
-- MAGIC **`VACUUM`** を実行し、上記の10ファイルを削除することで、これらのファイルが必要とされるテーブルのバージョンへのアクセスが永久に削除されます。

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------



-- COMMAND ----------

-- DBTITLE 0,--i18n-6574e909-c7ee-4b0b-afb8-8bac83dacdd3
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC テーブルディレクトリを確認して、ファイルが正常に削除されたことを示します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-3437d5f0-c0e2-4486-8142-413a1849bc40
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC このレッスンに関連するテーブルとファイルを削除するために、次のセルを実行してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
