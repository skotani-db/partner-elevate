-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-4c4121ee-13df-479f-be62-d59452a5f261
-- MAGIC %md
-- MAGIC
-- MAGIC # Databricks上のスキーマとテーブル
-- MAGIC このデモンストレーションでは、スキーマとテーブルを作成し、探索します。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このレッスンの最後に、次のことができるようになります：
-- MAGIC * Spark SQL DDLを使用してスキーマとテーブルを定義する
-- MAGIC * **`LOCATION`** キーワードがデフォルトの保存ディレクトリにどのように影響を与えるかを説明する
-- MAGIC
-- MAGIC **リソース**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">スキーマとテーブル - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">管理および非管理テーブル</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">UIを使用してテーブルを作成する</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">ローカルテーブルを作成する</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">永続テーブルへの保存</a>

-- COMMAND ----------

-- DBTITLE 0,--i18n-acb0c723-a2bf-4d00-b6cb-6e9aef114985
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## レッスンのセットアップ
-- MAGIC 次のスクリプトは、このデモの以前の実行をクリアし、SQLクエリで使用されるHive変数を設定します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.1

-- COMMAND ----------

-- DBTITLE 0,--i18n-cc3d2766-764e-44bb-a04b-b03ae9530b6d
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## スキーマ
-- MAGIC まず、スキーマ（データベース）を作成しましょう。

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;

-- COMMAND ----------

-- DBTITLE 0,--i18n-427db4b9-fa6c-47aa-ae70-b95087298362
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC  
-- MAGIC 注意：最初のスキーマ（データベース）の場所は、 **`dbfs:/user/hive/warehouse/`** のデフォルトの場所にあり、スキーマディレクトリはスキーマ名に **`.db`** 拡張子が付いています。

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED ${da.schema_name}_default_location;

-- COMMAND ----------

-- DBTITLE 0,--i18n-a0fda220-4a73-419b-969f-664dd4b80024
-- MAGIC %md
-- MAGIC ## 管理テーブル
-- MAGIC 場所のパスを指定しないことで、 **管理** テーブルを作成します。
-- MAGIC
-- MAGIC テーブルは、前述のスキーマ（データベース）内に作成します。
-- MAGIC
-- MAGIC データのカラムとデータ型を推測するデータがないため、テーブルのスキーマを定義する必要があります。

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT);
INSERT INTO managed_table 
VALUES (3, 2, 1);
SELECT * FROM managed_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c422056-45b4-419d-b4a6-2c3252e82575
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC 場所を見つけるには、テーブルの拡張情報を表示できます（結果内でスクロールする必要があります）。

-- COMMAND ----------

DESCRIBE DETAIL managed_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bdc6475c-1c77-46a5-9ea1-04d5a538c225
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC デフォルトでは、場所が指定されていないスキーマ内の**管理**テーブルは、**`dbfs:/user/hive/warehouse/<schema_name>.db/`** ディレクトリに作成されます。
-- MAGIC
-- MAGIC 予想通り、テーブルのデータとメタデータはその場所に格納されていることがわかります。
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_location = spark.sql(f"DESCRIBE DETAIL managed_table").first().location
-- MAGIC print(tbl_location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-507a84a5-f60f-4923-8f48-475ee3270dbd
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Drop the table.

-- COMMAND ----------

DROP TABLE managed_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-0b390bf4-3e3b-4d1a-bcb8-296fa1a7edb8
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC テーブルのディレクトリとそのログファイルおよびデータファイルは削除されます。スキーマ（データベース）ディレクトリだけが残ります。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC schema_default_location = spark.sql(f"DESCRIBE SCHEMA {DA.schema_name}_default_location").collect()[3].database_description_value
-- MAGIC print(schema_default_location)
-- MAGIC dbutils.fs.ls(schema_default_location)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC  
-- MAGIC ## 外部テーブル
-- MAGIC 次に、サンプルデータから　**外部**　（非管理）テーブルを作成します。
-- MAGIC
-- MAGIC 使用するデータはCSV形式です。指定したディレクトリ内の　**`LOCATION`**　を指定してDeltaテーブルを作成したいと思います。

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table; 

-- COMMAND ----------

-- DBTITLE 0,--i18n-6b5d7597-1fc1-4747-b5bb-07f67d806c2b
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC このレッスンの作業ディレクトリ内にテーブルのデータの場所をメモしましょう。

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-72f7bef4-570b-4c20-9261-b763b66b6942
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC さて、テーブルを削除します。

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-f71374ea-db51-4a2c-8920-9f8a000850df
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC テーブルの定義はもはやメタストアに存在しませんが、基になるデータはそのままです。

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-7defc948-a8e4-4019-9633-0886d653b7c6
-- MAGIC %md
-- MAGIC
-- MAGIC ## クリーンアップ
-- MAGIC スキーマを削除します。

-- COMMAND ----------

DROP SCHEMA ${da.schema_name}_default_location CASCADE;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb4a8ae9-450b-479f-9e16-a76f1131bd1a
-- MAGIC %md
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
