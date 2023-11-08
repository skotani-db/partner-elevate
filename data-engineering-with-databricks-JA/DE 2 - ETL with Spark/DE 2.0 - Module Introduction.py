# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-2fbad123-4065-4749-a925-d24f111ab27c
# MAGIC %md
# MAGIC ## Sparkを使用してデータを変換する
# MAGIC このモジュールは、Databricks Academyのデータエンジニア学習パスの一部で、SQLまたはPythonで受講できます。
# MAGIC
# MAGIC #### データの抽出
# MAGIC これらのノートブックは、SQLとPySparkの両方のユーザーに関連するSpark SQLの概念を示しています。
# MAGIC
# MAGIC [DE 2.1 - ファイルを直接クエリする]($./DE 2.1 - Querying Files Directly)  
# MAGIC [DE 2.2 - 外部ソースのオプションを提供する]($./DE 2.2 - Providing Options for External Sources)  
# MAGIC [DE 2.3L - データ抽出ラボ]($./DE 2.3L - Extract Data Lab)
# MAGIC
# MAGIC #### データの変換
# MAGIC これらのノートブックには、Spark SQLクエリとPySpark DataFrameコードが並列して含まれており、両方の言語で同じ概念を示しています。
# MAGIC
# MAGIC [DE 2.4 - データのクリーニング]($./DE 2.4 - Cleaning Data)  
# MAGIC [DE 2.5 - 複雑な変換]($./DE 2.5 - Complex Transformations)  
# MAGIC [DE 2.6L - データの再形成ラボ]($./DE 2.6L - Reshape Data Lab)
# MAGIC
# MAGIC #### その他の機能
# MAGIC
# MAGIC [DE 2.7A - SQL UDFs]($./DE 2.7A - SQL UDFs)  
# MAGIC [DE 2.7B - Python UDFs]($./DE 2.7B - Python UDFs)  
# MAGIC [DE 2.99 - 任意の高階関数（オプション）]($./DE 2.99 - OPTIONAL Higher Order Functions)  
# MAGIC
# MAGIC ### 前提条件
# MAGIC このコースの両方のバージョン（Spark SQLとPySpark）の前提条件：
# MAGIC * 基本的なクラウドコンセプト（仮想マシン、オブジェクトストレージ、アイデンティティ管理）についての初心者レベルの理解
# MAGIC * Databricksデータエンジニアリング＆データサイエンスワークスペースを使用した基本的なコード開発タスクの実行能力（クラスタの作成、ノートブックでのコードの実行、基本的なノートブック操作、gitからのリポジトリのインポートなど）
# MAGIC * 基本的なSQLコンセプト（選択、フィルタリング、グループ化、結合など）に対する中級者の理解
# MAGIC
# MAGIC PySparkバージョンのこのコースの追加の前提条件：
# MAGIC * Pythonでの初心者レベルのプログラミング経験（構文、条件、ループ、関数）
# MAGIC * Spark DataFrame APIを使用した初心者レベルのプログラミング経験：
# MAGIC * DataFrameReaderとDataFrameWriterを構成してデータを読み取り、書き込す
# MAGIC * DataFrameメソッドとColumn式を使用してクエリ変換を表現
# MAGIC * Sparkドキュメントをナビゲートして、さまざまな変換とデータ型のための組み込み関数を識別する能力
# MAGIC
# MAGIC 学生は、Spark DataFrame APIを使用したプログラミングの前提条件のスキルを学ぶために、Databricks Academyの「Introduction to PySpark Programming」コースを受講できます。<br>
# MAGIC
# MAGIC #### 技術的な考慮事項
# MAGIC * このコースはDBR 11.3で実行されます。
# MAGIC * このコースはDatabricks Community Editionでは提供されません。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
