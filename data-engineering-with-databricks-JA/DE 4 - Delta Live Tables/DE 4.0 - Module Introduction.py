# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-05f37e48-e8d1-4b0c-87e8-38cd4c42edc6
# MAGIC %md
# MAGIC
# MAGIC ## Delta Live Tablesを使用したデータパイプラインの構築
# MAGIC このモジュールは、Databricks Academyのデータエンジニアラーニングパスの一部です。
# MAGIC
# MAGIC #### DLT UI
# MAGIC
# MAGIC Lecture: The Medellion Architecture（講義：メデリオンアーキテクチャ） <br>
# MAGIC Lecture: Introduction to Delta Live Tables（講義：Delta Live Tablesへの導入） <br>
# MAGIC [DE 4.1 - Using the DLT UI]($./DE 4.1 - DLT UI Walkthrough) <br>
# MAGIC
# MAGIC #### DLT Syntax（DLT構文）
# MAGIC
# MAGIC DE 4.1.1 - Orders Pipeline: [SQL]($./DE 4.1A - SQL Pipelines/DE 4.1.1 - Orders Pipeline) または [Python]($./DE 4.1B - Python Pipelines/DE 4.1.1 - Orders Pipeline)<br>
# MAGIC DE 4.1.2 - Customers Pipeline: [SQL]($./DE 4.1A - SQL Pipelines/DE 4.1.2 - Customers Pipeline) または [Python]($./DE 4.1B - Python Pipelines/DE 4.1.2 - Customers Pipeline) <br>
# MAGIC [DE 4.2 - Python vs SQL]($./DE 4.2 - Python vs SQL) <br>
# MAGIC
# MAGIC #### パイプラインの結果、モニタリング、およびトラブルシューティング
# MAGIC
# MAGIC [DE 4.3 - Pipeline Results]($./DE 4.3 - Pipeline Results) <br>
# MAGIC [DE 4.4 - Pipeline Event Logs]($./DE 4.4 - Pipeline Event Logs) <br>
# MAGIC DE 4.1.3 - Status Pipeline: [SQL]($./DE 4.1A - SQL Pipelines/DE 4.1.3 - Status Pipeline) または [Python]($./DE 4.1B - Python Pipelines/DE 4.1.3 - Status Pipeline) <br>
# MAGIC [DE 4.99 - Land New Data]($./DE 4.99 - Land New Data) <br>
# MAGIC
# MAGIC #### 必要な前提条件
# MAGIC
# MAGIC * クラウドコンピューティングの基本的な概念についての初級の理解（仮想マシン、オブジェクトストレージなど）
# MAGIC * Databricksデータエンジニアリング＆データサイエンスワークスペースを使用して基本的なコード開発タスクを実行できること（クラスタの作成、ノートブックでのコード実行、基本的なノートブック操作、Gitからのリポジトリのインポートなど）
# MAGIC * Delta Lakeの初級プログラミング経験
# MAGIC   * Delta Lake DDLを使用してテーブルを作成し、ファイルを最適化し、以前のテーブルバージョンを復元し、Lakehouseのテーブルのガベージコレクションを実行する
# MAGIC   * CTASを使用して、クエリから派生したデータをDelta Lakeテーブルに保存する
# MAGIC   * SQLを使用して既存のテーブルに対して完全および増分の更新を実行する
# MAGIC * Spark SQLまたはPySparkの初級プログラミング経験
# MAGIC   * さまざまなファイル形式およびデータソースからデータを抽出する
# MAGIC   * 一般的な変換を適用してデータをクリーンにする
# MAGIC   * 高度な組み込み関数を使用して複雑なデータを整形および操作する
# MAGIC * データウェアハウスおよびデータレイクでのプロダクション経験
# MAGIC
# MAGIC #### 技術的な考慮事項
# MAGIC
# MAGIC * このコースはDBR 11.3で実行されます。
# MAGIC * このコースはDatabricks Community Editionでは提供できません。
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
