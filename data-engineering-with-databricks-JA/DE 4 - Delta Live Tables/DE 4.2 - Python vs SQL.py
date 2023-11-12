# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-6ee8963d-1bec-43df-9729-b4da38e8ad0d
# MAGIC %md
# MAGIC # デルタライブテーブル：PythonとSQLの比較
# MAGIC
# MAGIC このレッスンでは、デルタライブテーブルのPython実装とSQL実装の主な違いについて見ていきます。
# MAGIC
# MAGIC このレッスンを終える頃には、以下のことができるようになります：
# MAGIC
# MAGIC * デルタライブテーブルのPython実装とSQL実装の主な違いを特定する

# COMMAND ----------

# DBTITLE 0,--i18n-48342971-ca6a-4774-88e1-951b8189bec4
# MAGIC %md
# MAGIC
# MAGIC # PythonとSQLの比較
# MAGIC | Python | SQL | 備考 |
# MAGIC |--------|--------|--------|
# MAGIC | Python API | 独自のSQL API |  |
# MAGIC | 構文チェックなし | 構文チェックあり | Pythonでは、DLTノートブックのセルを単独で実行するとエラーが表示されますが、SQLではコマンドが文法的に有効かどうかをチェックして教えてくれます。どちらの場合も、個々のノートブックのセルはDLTパイプラインで実行することは想定されていません。 |
# MAGIC | インポートに関する注意 | なし | dltモジュールはPythonノートブックライブラリに明示的にインポートする必要があります。SQLではこれは必要ありません。 |
# MAGIC | テーブルをデータフレームとして | テーブルをクエリ結果として | PythonのDataFrame APIでは、複数のAPIコールを連結することにより、データセットの複数の変換を行うことができます。これに対してSQLでは、同じ変換を行う場合は、それらを一時的なテーブルに保存する必要があります。 |
# MAGIC | @dlt.table() | SELECT文 | SQLでは、データに対する変換を含むクエリの主要なロジックがSELECT文に含まれます。Pythonでは、データ変換は@dlt.table()のオプションを設定する際に指定されます。 |
# MAGIC | @dlt.table(comment = "Pythonのコメント",table_properties = {"quality": "silver"}) | COMMENT "SQLのコメント" TBLPROPERTIES ("quality" = "silver") | PythonとSQLでコメントとテーブルプロパティを追加する方法です。 |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
