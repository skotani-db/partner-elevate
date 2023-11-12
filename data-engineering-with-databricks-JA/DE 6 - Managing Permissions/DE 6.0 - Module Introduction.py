# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-26e0e9c0-768d-4b35-9e7c-edf0bbca828b
# MAGIC %md
# MAGIC ## アナリティクスのためのデータアクセスの管理
# MAGIC このモジュールは、Databricks Academyのデータエンジニアリングラーニングパスの一部です。
# MAGIC
# MAGIC #### レッスン
# MAGIC 講義：Unityカタログの紹介<br>
# MAGIC DE 6.1 - [UCを使用してデータを作成および管理する]($./DE 6.1 - Create and Govern Data with UC)<br>
# MAGIC DE 6.2L - [Unityカタログでテーブルを作成および共有する]($./DE 6.2L - Create and Share Tables in Unity Catalog)<br>
# MAGIC DE 6.3L - [ビューの作成とテーブルアクセスの制限]($./DE 6.3L - Create Views and Limit Table Access)<br>
# MAGIC
# MAGIC
# MAGIC #### Unityカタログを使用した管理 - オプション
# MAGIC DE 6.99 - オプションの管理<br>
# MAGIC DE 6.99.2 - [Unityカタログアクセスのためのコンピュートリソースの作成]($./DE 6.99 - OPTIONAL Administration/DE 6.99.2 - Create compute resources for Unity Catalog access)<br>
# MAGIC DE 6.99.3 - [テーブルをUnityカタログにアップグレード]($./DE 6.99 - OPTIONAL Administration/DE 6.99.3 - OPTIONAL Upgrade a Table to Unity Catalog)<br>
# MAGIC
# MAGIC
# MAGIC #### 必要条件
# MAGIC * Databricks Lakehouseプラットフォームに関する初級レベルの知識（Lakehouseプラットフォームの構造と利点に関する高レベルの知識）
# MAGIC * SQLの初級レベルの知識（基本的なクエリの理解と構築の能力）
# MAGIC
# MAGIC
# MAGIC #### 技術的考慮事項
# MAGIC このコースは、Databricks Community Editionでは提供できず、Unityカタログをサポートするクラウドでのみ提供できます。すべての演習を完全に実行するには、ワークスペースとアカウントのレベルでの管理アクセスが必要です。クラウド環境への低レベルのアクセスも必要とするいくつかのオプションのタスクが実演されています。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
