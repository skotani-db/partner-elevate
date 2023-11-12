# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-2d116d36-8ed8-44aa-8cfa-156e16f88492
# MAGIC %md
# MAGIC # DLTパイプラインの結果を探る
# MAGIC
# MAGIC DLT（Delta Live Tables）は、Databricks上での本番ETL（Extract, Transform, Load）を実行する際の多くの複雑さを抽象化していますが、多くの人々はその内部で実際に何が起こっているのか疑問に思うかもしれません。
# MAGIC
# MAGIC このノートブックでは、詳細には立ち入りませんが、DLTによってデータとメタデータがどのように永続化されるかを探ります。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.3

# COMMAND ----------

# DBTITLE 0,--i18n-ee147dd8-867c-44a7-a4d6-964a5178e8ff
# MAGIC %md
# MAGIC ## ターゲットデータベース内のテーブルを照会する
# MAGIC
# MAGIC DLTパイプラインの設定中にターゲットデータベースが指定されている限り、そのテーブルはDatabricks環境全体のユーザーが利用可能です。
# MAGIC
# MAGIC 以下のセルを実行して、このデモで使用されているデータベースに登録されているテーブルを確認してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${DA.schema_name};
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# DBTITLE 0,--i18n-d5368f5c-7a7d-41e6-b6a2-7a6af2d95c15
# MAGIC %md
# MAGIC パイプラインで定義したビューがテーブルリストから欠落していることに注意してください。
# MAGIC
# MAGIC **`orders_bronze`** テーブルからのクエリ結果を確認してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bronze

# COMMAND ----------

# DBTITLE 0,--i18n-a6a09270-4f8e-4f17-95ab-5e82220a83ed
# MAGIC %md
# MAGIC **`orders_bronze`** はDLT内でストリーミングライブテーブルとして定義されましたが、ここでの結果は静的です。
# MAGIC
# MAGIC DLTはすべてのテーブルをデルタレイクに保存しているため、クエリが実行されるたびに、常にテーブルの最新バージョンが返されます。しかし、DLT外部のクエリは、テーブルがどのように定義されていたかに関わらず、DLTテーブルからスナップショット結果を返します。

# COMMAND ----------

# DBTITLE 0,--i18n-9439da5b-7ab5-4b31-a66d-ea50040f2501
# MAGIC %md
# MAGIC ## `APPLY CHANGES INTO` の結果を調べる
# MAGIC
# MAGIC **`customers_silver`** テーブルは、Type 1 SCDとしてCDCフィードからの変更が適用されて実装されたことを思い出してください。
# MAGIC
# MAGIC 以下でこのテーブルをクエリしてみましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_silver

# COMMAND ----------

# DBTITLE 0,--i18n-20b2f8b4-9a16-4a8a-b63f-c974e9ab167f
# MAGIC %md
# MAGIC
# MAGIC **`customers_silver`** テーブルは、適用された変更を含めて、Type 1テーブルの現在のアクティブな状態を正しく表しています。しかし、実際には **customers_silver** テーブルは、隠されたテーブル **__apply_changes_storage_customers_silver** に対するビューとして実装されており、追加のフィールド **__Timestamp**、**__DeleteVersion**、および **__UpsertVersion** が含まれています。
# MAGIC
# MAGIC これは **`DESCRIBE EXTENDED`** を実行することで確認できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customers_silver

# COMMAND ----------

# DBTITLE 0,--i18n-6c70c0ce-abdd-4dab-99fb-661324056120
# MAGIC %md
# MAGIC この隠されたテーブルにクエリを実行すると、これら3つのフィールドが表示されます。ただし、このテーブルはDLTによってアップデートが正しい順序で適用され、結果が正しく具体化されることを保証するために利用されるだけなので、ユーザーが直接このテーブルとやり取りする必要はありません。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM __apply_changes_storage_customers_silver

# COMMAND ----------

# DBTITLE 0,--i18n-6c64b04a-77be-4a2b-9d2b-dd7978b213f7
# MAGIC %md
# MAGIC ## データファイルの調査
# MAGIC
# MAGIC 設定された**ストレージの場所**でファイルを調べるために、以下のセルを実行します。

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-54bf6537-bf01-4963-9b18-f16c4b2f7692
# MAGIC %md
# MAGIC **autoloader** と **checkpoint** ディレクトリには、Structured Streamingによるインクリメンタルデータ処理を管理するために使用されるデータが含まれています。
# MAGIC
# MAGIC **system** ディレクトリは、パイプラインに関連するイベントを記録します。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-a459d740-2091-40e0-8b47-d67ecdb2fd8e
# MAGIC %md
# MAGIC これらのイベントログはデルタテーブルとして保存されます。このテーブルにクエリを実行してみましょう。

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{DA.paths.storage_location}/system/events`"))

# COMMAND ----------

# DBTITLE 0,--i18n-61fd77b8-9bd6-4440-a37a-f45169fbf4c0
# MAGIC %md
# MAGIC 次のノートブックでメトリクスについてより詳しく見ていきましょう。
# MAGIC
# MAGIC **tables** ディレクトリの内容を見てみましょう。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-a36ca049-9586-4551-8988-c1b8ec1da349
# MAGIC %md
# MAGIC これらのディレクトリのそれぞれには、DLTによって管理されているデルタレイクテーブルが含まれています。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
