# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-dccf35eb-f70e-4271-ad22-3a2f10837a13
# MAGIC %md
# MAGIC ## Delta Lakeを使用したデータ管理
# MAGIC このモジュールは、Databricks Academyのデータエンジニア向けラーニングパスの一部です。
# MAGIC
# MAGIC #### レッスン
# MAGIC 講義: Delta Lakeとは何か<br>
# MAGIC [DE 3.1 - スキーマとテーブル]($./DE 3.1 - Schemas and Tables) <br>
# MAGIC [DE 3.2 - Deltaテーブルのセットアップ]($./DE 3.2 - Set Up Delta Tables) <br>
# MAGIC [DE 3.3 - Delta Lakeにデータをロード]($./DE 3.3 - Load Data into Delta Lake) <br>
# MAGIC [DE 3.4 - データをロードするラボ]($./DE 3.4L - Load Data Lab) <br>
# MAGIC [DE 3.5 - Deltaテーブルのバージョンと最適化]($./DE 3.5 - Version and Optimize Delta Tables) <br>
# MAGIC [DE 3.6 - Deltaテーブルを操作するラボ]($./DE 3.6L - Manipulate Delta Tables Lab) <br>
# MAGIC
# MAGIC #### 前提条件
# MAGIC * クラウドコンピューティングの基本的な知識（仮想マシン、オブジェクトストレージなど）
# MAGIC * Databricksデータエンジニアリング＆データサイエンスワークスペースを使用して基本的なコード開発タスクを実行できる能力（クラスターの作成、ノートブックでのコード実行、基本的なノートブック操作、gitからのリポジトリのインポートなど）
# MAGIC * Spark SQLでのプログラミング経験の初級者
# MAGIC   * 様々なファイルフォーマットおよびデータソースからデータを抽出
# MAGIC   * データをクリーニングするための一般的な変換を適用
# MAGIC   * 高度な組み込み関数を使用して複雑なデータを整形および操作
# MAGIC
# MAGIC #### 技術的な考慮事項
# MAGIC * このコースはDBR 11.3で実行されます。
# MAGIC * このコースはDatabricks Community Editionでは提供できません。
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
