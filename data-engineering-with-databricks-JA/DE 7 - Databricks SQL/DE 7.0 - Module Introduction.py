# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-e263b6d4-ac6b-42c3-9c79-086e0881358d
# MAGIC %md
# MAGIC ## Databricks SQL
# MAGIC このモジュールは、Databricks Academyによるデータエンジニアの学習パスの一部です。
# MAGIC
# MAGIC #### レッスン
# MAGIC [DE 7.1 - Databricks SQLのナビゲーションとWarehouseへのアタッチ]($./DE 7.1 - Databricks SQLのナビゲーションとWarehouseへのアタッチ) <br>
# MAGIC [DE 7.2L - DBSQLを使用したラストマイルETL]($./DE 7.2L - DBSQLを使用したラストマイルETL) <br>
# MAGIC
# MAGIC #### 必要条件
# MAGIC * Databricksデータエンジニアリング＆データサイエンスワークスペースで基本的なコード開発タスクを実行できる能力（クラスタの作成、ノートブックでのコード実行、基本的なノートブック操作、gitからのリポジトリのインポートなど）
# MAGIC * Delta Live Tables UIを使用してデータパイプラインを設定および実行できる能力
# MAGIC * PySparkを使用してDelta Live Tables（DLT）パイプラインを定義する初心者の経験
# MAGIC * Auto LoaderおよびPySpark構文を使用してデータを取り込み、処理できる能力
# MAGIC * APPLY CHANGES INTO構文を使用して変更データキャプチャフィードを処理できる能力
# MAGIC * DLT構文のトラブルシューティングのためにパイプラインイベントログと結果を確認できる能力
# MAGIC * データウェアハウスとデータレイクでのプロダクション経験
# MAGIC
# MAGIC #### 技術的な注意事項
# MAGIC * このコースはDBR 11.3で実行されます。
# MAGIC * このコースはDatabricks Community Editionでは提供できません。
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
