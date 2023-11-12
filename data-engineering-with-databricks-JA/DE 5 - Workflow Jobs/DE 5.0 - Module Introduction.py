# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-8d3d2aed-1539-4db1-8e52-0aa71a4ecc9d
# MAGIC %md
# MAGIC ## Databricksワークフロージョブによるオーケストレーション
# MAGIC このモジュールは、Databricks Academyのデータエンジニアリングラーニングパスの一部です。
# MAGIC
# MAGIC #### レッスン
# MAGIC 講義：ワークフローの紹介 <br>
# MAGIC 講義：ワークフロージョブの構築と監視 <br>
# MAGIC デモ：ワークフロージョブの構築と監視 <br>
# MAGIC DE 5.1 - ジョブUIを使用したタスクのスケジュール <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.1 - タスクのオーケストレーション]($./DE 5.1 - ジョブUIを使用したタスクのスケジュール/DE 5.1.1 - タスクのオーケストレーション) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.2 - リセット]($./DE 5.1 - ジョブUIを使用したタスクのスケジュール/DE 5.1.2 - リセット) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.3 - DLTジョブ]($./DE 5.1 - ジョブUIを使用したタスクのスケジュール/DE 5.1.3 - DLTジョブ) <br>
# MAGIC DE 5.2L - ジョブラボ <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.1L - ラボの手順]($./DE 5.2L - ジョブラボ/DE 5.2.1L - ラボの手順) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.2L - バッチジョブ]($./DE 5.2L - ジョブラボ/DE 5.2.2L - バッチジョブ) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.3L - DLTジョブ]($./DE 5.2L - ジョブラボ/DE 5.2.3L - DLTジョブ) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.4L - クエリ結果ジョブ]($./DE 5.2L - ジョブラボ/DE 5.2.4L - クエリ結果ジョブ) <br>
# MAGIC
# MAGIC #### 必要条件
# MAGIC * Databricksデータエンジニアリング＆データサイエンスワークスペースを使用して基本的なコード開発タスクを実行できる能力（クラスタの作成、ノートブックでのコード実行、基本的なノートブック操作、Gitからリポジトリのインポートなど）
# MAGIC * Delta Live Tables UIを使用してデータパイプラインを構成および実行できる能力
# MAGIC * PySparkを使用してDelta Live Tables（DLT）パイプラインを定義する初心者の経験
# MAGIC   * Auto LoaderおよびPySpark構文を使用してデータを取り込み、処理する
# MAGIC   * APPLY CHANGES INTO構文を使用してChange Data Captureフィードを処理する
# MAGIC * DLT構文のトラブルシューティングのためにパイプラインイベントログと結果を確認する能力
# MAGIC * データウェアハウスとデータレイクでのプロダクション経験
# MAGIC
# MAGIC #### 技術的考慮事項
# MAGIC * このコースはDBR 11.3で実行されます。
# MAGIC * このコースはDatabricks Community Editionでは提供できません。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
