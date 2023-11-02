-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-65583202-79bf-45b7-8327-d4d5562c831d
-- MAGIC %md
-- MAGIC
-- MAGIC # Deltaテーブルの操作ラボ
-- MAGIC
-- MAGIC このノートブックでは、データレイクハウスにDelta Lakeがもたらすいくつかの高度な機能について実際の操作を行います。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このラボの終わりに、以下のことができるようになるはずです。
-- MAGIC - テーブルの履歴を確認する
-- MAGIC - 以前のテーブルバージョンをクエリし、テーブルを特定のバージョンにロールバックする
-- MAGIC - ファイルのコンパクションとZオーダーインデックスの実行
-- MAGIC - 永久削除のマークがされたファイルをプレビューし、これらの削除をコミットする

-- COMMAND ----------

-- DBTITLE 0,--i18n-065e2f94-2251-4701-b0b6-f4b86323dec8
-- MAGIC %md
-- MAGIC
-- MAGIC ## セットアップ
-- MAGIC 以下のスクリプトを実行して、必要な変数を設定し、このノートブックの過去の実行をクリアしてください。このセルを再実行することで、ラボを最初からやり直すことができます。
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.3L

-- COMMAND ----------

-- DBTITLE 0,--i18n-56940be8-afa9-49d8-8949-b4bcdb343f9d
-- MAGIC %md
-- MAGIC
-- MAGIC ## ビーンコレクションの履歴を作成
-- MAGIC
-- MAGIC 以下のセルにはさまざまなテーブル操作が含まれており、**`beans`** テーブルの次のスキーマが生成されます：
-- MAGIC
-- MAGIC | フィールド名 | フィールドタイプ |
-- MAGIC | --- | --- |
-- MAGIC | name | STRING |
-- MAGIC | color | STRING |
-- MAGIC | grams | FLOAT |
-- MAGIC | delicious | BOOLEAN |
-- MAGIC

-- COMMAND ----------

CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false);

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false);

UPDATE beans
SET delicious = true
WHERE name = "jelly";

UPDATE beans
SET grams = 1500
WHERE name = 'pinto';

DELETE FROM beans
WHERE delicious = false;

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

MERGE INTO beans a
USING new_beans b
ON a.name=b.name AND a.color = b.color
WHEN MATCHED THEN
  UPDATE SET grams = a.grams + b.grams
WHEN NOT MATCHED AND b.delicious = true THEN
  INSERT *;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bf6ff074-4166-4d51-92e5-67e7f2084c9b
-- MAGIC %md
-- MAGIC
-- MAGIC ## テーブルの履歴を確認
-- MAGIC
-- MAGIC Delta Lake のトランザクションログは、テーブルの内容や設定を変更する各トランザクションに関する情報を保存します。
-- MAGIC
-- MAGIC 以下は **`beans`** テーブルの履歴を確認します。
-- MAGIC

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- DBTITLE 0,--i18n-fb56d746-8889-41c1-ba73-576282582534
-- MAGIC %md
-- MAGIC
-- MAGIC すべての前回の操作が説明通りに完了した場合、テーブルのバージョンが7つ表示されるはずです（**注意**: Delta Lake のバージョニングは 0 から始まるため、最大バージョン番号は 6 になります）。
-- MAGIC
-- MAGIC 操作は次のとおりです。
-- MAGIC
-- MAGIC | バージョン | 操作 |
-- MAGIC | --- | --- |
-- MAGIC | 0 | CREATE TABLE |
-- MAGIC | 1 | WRITE |
-- MAGIC | 2 | WRITE |
-- MAGIC | 3 | UPDATE |
-- MAGIC | 4 | UPDATE |
-- MAGIC | 5 | DELETE |
-- MAGIC | 6 | MERGE |
-- MAGIC
-- MAGIC **`operationsParameters`** 列には、更新、削除、およびマージに使用される述語を確認できます。 
-- MAGIC **`operationMetrics`** 列は、各操作で追加される行数とファイル数を示します。
-- MAGIC
-- MAGIC Delta Lake の履歴を確認して、特定のトランザクションと一致するテーブルバージョンを理解するために時間をかけてください。
-- MAGIC
-- MAGIC **注意**: **`version`** 列は、特定のトランザクションが完了した時点でのテーブルの状態を指定します。 **`readVersion`** 列は、実行される操作対象のテーブルのバージョンを示します。このシンプルなデモでは（同時トランザクションがない場合）、この関係は常に 1 ずつ増加するはずです。
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-00d8e251-9c9e-4be3-b8e7-6e38b07fac55
-- MAGIC %md
-- MAGIC
-- MAGIC ## 特定のバージョンをクエリ
-- MAGIC
-- MAGIC テーブルの履歴を確認した後、最初のデータが挿入された直後のテーブルの状態を表示したいと思った場合、以下のクエリを実行してください。
-- MAGIC

-- COMMAND ----------

SELECT * FROM beans VERSION AS OF 1

-- COMMAND ----------

-- DBTITLE 0,--i18n-90e3c115-6bed-4b83-bb37-dd45fb92aec5
-- MAGIC %md
-- MAGIC
-- MAGIC そして、データの現在の状態を確認してください。
-- MAGIC

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-f073a6d9-3aca-41a0-9452-a278fb87fa8c
-- MAGIC %md
-- MAGIC
-- MAGIC データが削除される直前のバージョンを一時的なビューとして登録し、次のセルを実行してビューをクエリするために、以下のステートメントを入力してください。
-- MAGIC

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW pre_delete_vw AS
<FILL-IN>

-- COMMAND ----------

SELECT * FROM pre_delete_vw

-- COMMAND ----------

-- DBTITLE 0,--i18n-bad13c31-d91f-454e-a14e-888d255dc8a4
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 指定されたバージョンが正しいことを確認するために、以下のセルを実行してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("pre_delete_vw"), "Make sure you have registered the temporary view with the provided name `pre_delete_vw`"
-- MAGIC assert spark.table("pre_delete_vw").count() == 6, "Make sure you're querying a version of the table with 6 records"
-- MAGIC assert spark.table("pre_delete_vw").selectExpr("int(sum(grams))").first()[0] == 43220, "Make sure you query the version of the table after updates were applied"

-- COMMAND ----------

-- DBTITLE 0,--i18n-8450d1ef-c49b-4c67-9390-3e0550c9efbc
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## 以前のバージョンに戻す
-- MAGIC
-- MAGIC 恐らく、友達からもらったあなたのコレクションにマージした豆は、あなたが保持するためではなかったようです。
-- MAGIC
-- MAGIC この **`MERGE`** ステートメントが完了した前のバージョンにテーブルを戻します。 
-- MAGIC

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- DBTITLE 0,--i18n-405edc91-49e8-412b-99e7-96cc60aab32d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC テーブルの履歴を確認し、以前のバージョンに戻すと新しいテーブルバージョンが追加されることに注意してください。

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
-- MAGIC assert spark.sql(f"DESCRIBE HISTORY beans").select("operation").first()[0] == "RESTORE", "Make sure you reverted your table with the `RESTORE` keyword"
-- MAGIC assert spark.table("beans").count() == 5, "Make sure you reverted to the version after deleting records but before merging"

-- COMMAND ----------

-- DBTITLE 0,--i18n-d430fe1c-32f1-44c0-907a-62ef8a5ca07b
-- MAGIC %md
-- MAGIC
-- MAGIC ## ファイルの圧縮
-- MAGIC 履歴を見ていると、データのコレクションが小さなものにもかかわらず、非常に多くのファイルがあることに驚かされます。
-- MAGIC
-- MAGIC このサイズのテーブルにインデックスを追加しても性能が向上する可能性は低いですが、将来的にビーンのコレクションが急増することを考えて、**`name`** フィールドに Z-order インデックスを追加することに決めました。
-- MAGIC
-- MAGIC 以下のセルを使用してファイルの圧縮と Z-order インデックスの作成を行います。

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- DBTITLE 0,--i18n-8ef4ffb6-c958-4798-b564-fd2e65d4fa0e
-- MAGIC %md
-- MAGIC
-- MAGIC データは単一のファイルに圧縮されたはずです。これを確認するために、以下のセルを実行してください。

-- COMMAND ----------

DESCRIBE DETAIL beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-8a63081c-1423-43f2-9608-fe846a4a58bb
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Run the cell below to check that you've successfully optimized and indexed your table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").first()
-- MAGIC assert last_tx["operation"] == "OPTIMIZE", "Make sure you used the `OPTIMIZE` command to perform file compaction"
-- MAGIC assert last_tx["operationParameters"]["zOrderBy"] == '["name"]', "Use `ZORDER BY name` with your optimize command to index your table"

-- COMMAND ----------

-- DBTITLE 0,--i18n-6432b28c-18c1-4402-864c-ea40abca50e1
-- MAGIC %md
-- MAGIC
-- MAGIC ## 不要なデータファイルのクリーンアップ
-- MAGIC
-- MAGIC すべてのデータが1つのデータファイルに格納されていることを知っていますが、以前のバージョンのテーブルのデータファイルはまだ隣接して保存されています。これらのファイルを削除し、テーブルの以前のバージョンへのアクセスを削除するため、テーブルに **`VACUUM`** を実行することを希望します。
-- MAGIC
-- MAGIC **`VACUUM`** を実行すると、テーブルディレクトリ内のゴミ掃除が行われます。デフォルトでは、7日間の保持期間が適用されます。
-- MAGIC
-- MAGIC 以下のセルでは、いくつかのSpark設定を変更します。最初のコマンドは、データの永久的な削除をデモンストレーションするために保持期間チェックを無効にします。
-- MAGIC
-- MAGIC **注意**: 短い保持期間で本番テーブルに **`VACUUM`** を実行すると、データの破損や長時間実行されるクエリの失敗が発生する可能性があります。これはデモンストレーション専用であり、この設定を無効にする場合は非常に注意が必要です。
-- MAGIC
-- MAGIC 2番目のコマンドは、 **`spark.databricks.delta.vacuum.logging.enabled`** を **`true`** に設定し、 **`VACUUM`** 操作がトランザクションログに記録されることを確認します。
-- MAGIC
-- MAGIC **注意** : DBR 9.1時点では、一部のクラウドではデフォルトでは **`VACUUM`** コマンドのログ記録が無効になっている場合があるため、ストレージプロトコルのわずかな違いによります。
-- MAGIC

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- COMMAND ----------

-- DBTITLE 0,--i18n-04f27ab4-7848-4418-ac79-c339f9843b23
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Before permanently deleting data files, review them manually using the **`DRY RUN`** option.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb9ce589-09ae-47b8-b6a6-4ab8e4dc70e7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 以前のテーブルバージョンに存在しないすべてのデータファイルが、上記のプレビューに表示されます。
-- MAGIC
-- MAGIC これらのファイルを永久に削除するには、**`DRY RUN`** を削除してコマンドを再実行してください。
-- MAGIC
-- MAGIC **注意**: 以前のすべてのテーブルバージョンにアクセスできなくなります。

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-1630420a-94f5-43eb-b37c-ccbb46c9ba40
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC **`VACUUM`** は重要なデータセットに対して破壊的な操作であるため、保持期間チェックを再度有効にすることは常に良いアイデアです。以下のセルを実行して、この設定を再アクティブ化してください。

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = true

-- COMMAND ----------

-- DBTITLE 0,--i18n-8d72840f-49f1-4983-92e4-73845aa98086
-- MAGIC %md
-- MAGIC
-- MAGIC テーブルの履歴には、 **`VACUUM`** 操作を実行したユーザー、削除されたファイルの数、およびこの操作中に保持期間チェックが無効になったことが記録されます。

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-875b39be-103c-4c70-8a2b-43eaa4a513ee
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC テーブルを再度クエリして、現在のバージョンにアクセスできることを確認します。

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-fdb81194-2e3e-4a00-bfb6-97e822ae9ec3
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> Delta Cacheは、現在のセッションでクエリされたファイルのコピーを、現在アクティブなクラスタに展開されたストレージボリュームに保存するため、一時的に前のテーブルバージョンにアクセスできる場合があります（ただし、システムはこの動作を期待するように設計されていては**ありません**）。
-- MAGIC
-- MAGIC クラスタを再起動することで、これらのキャッシュされたデータファイルが永久に削除されることが保証されます。
-- MAGIC
-- MAGIC 以下のセルのコメントを解除し、実行することで、これがキャッシュの状態に依存して成功するかどうかを確認できます。
-- MAGIC

-- COMMAND ----------

-- SELECT * FROM beans@v1

-- COMMAND ----------

-- DBTITLE 0,--i18n-a5cfd876-c53a-4d60-96e2-cdbc2b00c19f
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC このラボを完了することで、次の操作に自信を持つようになるはずです：
-- MAGIC * 標準的なDelta Lakeテーブルの作成およびデータ操作コマンドを完了する
-- MAGIC * テーブル履歴を含むテーブルのメタデータを確認する
-- MAGIC * スナップショットクエリとロールバックにDelta Lakeのバージョニングを活用する
-- MAGIC * 小さなファイルをコンパクションし、テーブルをインデックス化する
-- MAGIC * 削除予定のファイルを確認し、これらの削除をコミットするために **`VACUUM`** を使用する

-- COMMAND ----------

-- DBTITLE 0,--i18n-b541b92b-03a9-4f3c-b41c-fdb0ce4f2271
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC 以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除します。
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
