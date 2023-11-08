# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-67600b38-db2e-44e0-ba4a-1672ee796c77
# MAGIC %md
# MAGIC # インタラクティブクラスタの作成と管理
# MAGIC
# MAGIC Databricksクラスタは、データエンジニアリング、データサイエンス、データ分析のワークロードを実行するための計算リソースと設定のセットです。これには、本番のETLパイプライン、ストリーミング分析、アドホック分析、機械学習などが含まれます。これらのワークロードは、ノートブック内の一連のコマンドまたは自動化されたジョブとして実行します。
# MAGIC
# MAGIC Databricksでは、汎用クラスタとジョブクラスタの区別があります。
# MAGIC - 汎用クラスタは対話型のノートブックを使用してデータを共同で分析するために使用します。
# MAGIC - ジョブクラスタは高速で堅牢な自動ジョブを実行するために使用します。
# MAGIC
# MAGIC このデモでは、Databricksデータサイエンス＆エンジニアリングワークスペースを使用して汎用Databricksクラスタを作成および管理する方法を説明します。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの最後までに、次のことができるようになるはずです：
# MAGIC * クラスタを構成およびデプロイするためにクラスタUIを使用する
# MAGIC * クラスタを編集、終了、再起動、削除する

# COMMAND ----------

# DBTITLE 0,--i18n-81a87f46-ce1b-482f-9ad8-62418db25665
# MAGIC %md
# MAGIC ## クラスタの作成
# MAGIC
# MAGIC 現在作業しているワークスペースによっては、クラスタの作成権限があるかどうかが異なる場合があります。
# MAGIC
# MAGIC このセクションの手順では、クラスタの作成権限があると仮定し、このコースのレッスンを実行するために新しいクラスタを展開する必要があるとします。
# MAGIC
# MAGIC **注意**: クラスタの構成オプションに影響を与える場合があるため、インストラクターまたはプラットフォーム管理者に確認して、新しいクラスタを作成するか、既に展開されたクラスタに接続するかどうかを確認してください。
# MAGIC
# MAGIC 手順：
# MAGIC 1. 左側のサイドバーを使用して、![compute](https://files.training.databricks.com/images/clusters-icon.png) アイコンをクリックして **Compute** ページに移動します
# MAGIC 1. 青い **Create Cluster** ボタンをクリックします
# MAGIC 1. **Cluster name** に、自分の名前を使用して、簡単に見つけられ、問題がある場合にインストラクターが簡単に識別できるようにします
# MAGIC 1. **Cluster mode** を **Single Node** に設定します（このモードはこのコースを実行するために必要です）
# MAGIC 1. このコース用に推奨される **Databricks runtime version** を使用します
# MAGIC 1. **Autopilot Options** のデフォルト設定にチェックマークを付けたままにします
# MAGIC 1. 青い **Create Cluster** ボタンをクリックします
# MAGIC
# MAGIC **注意:** クラスタを展開するには数分かかることがあります。クラスタの展開が完了したら、クラスタの作成UIを引き続き探索してみてください。

# COMMAND ----------

# DBTITLE 0,--i18n-11f7b691-6ba9-49d5-b975-2924a44d05d1
# MAGIC %md
# MAGIC
# MAGIC ### <img src="https://files.training.databricks.com/images/icon_warn_24.png"> このコースではシングルノードクラスタが必要です
# MAGIC **重要:** このコースでは、シングルノードクラスタでノートブックを実行する必要があります。
# MAGIC
# MAGIC 上記の手順に従って、**Cluster mode** を **`Single Node`** に設定したクラスタを作成してください。

# COMMAND ----------

# DBTITLE 0,--i18n-7323201d-6d28-4780-b2f7-47ab22eadb8f
# MAGIC %md
# MAGIC ## クラスタの管理
# MAGIC
# MAGIC クラスタが作成されたら、クラスタを表示するために **Compute** ページに戻ります。
# MAGIC
# MAGIC クラスタを選択して現在の構成を確認します。
# MAGIC
# MAGIC  **編集** ボタンをクリックします。ほとんどの設定は変更できることに注意してください（適切な権限がある場合）。ほとんどの設定を変更するには、実行中のクラスタを再起動する必要があります。
# MAGIC
# MAGIC  **注意** : 次のレッスンでクラスタを使用します。クラスタを再起動、終了、または削除すると、新しいリソースが展開されるのを待つ間、遅れる可能性があります。

# COMMAND ----------

# DBTITLE 0,--i18n-2fafe840-a86f-4bd7-9d60-7044610b8d5a
# MAGIC
# MAGIC %md
# MAGIC ## 再起動、終了、および削除
# MAGIC
# MAGIC 注意: **再起動**、**終了**、および**削除**には異なる効果がありますが、すべてクラスタ終了イベントから始まります（この設定を使用している場合、クラスタは非アクティブになったため自動的に終了します）。
# MAGIC
# MAGIC クラスタが終了すると、現在使用中のすべてのクラウドリソースが削除されます。これは次のことを意味します。
# MAGIC * 関連するVMおよび動作中のメモリが削除されます
# MAGIC * アタッチされたボリュームストレージが削除されます
# MAGIC * ノード間のネットワーク接続が削除されます
# MAGIC
# MAGIC 要するに、以前の計算環境に関連付けられたすべてのリソースが完全に削除されます。これは、**永続化が必要な結果は永続的な場所に保存する必要がある**ことを意味します。コードは失われませんし、適切に保存されたデータファイルも失われません。
# MAGIC
# MAGIC **再起動**ボタンを使用すると、クラスタを手動で再起動できます。これは、クラスタのキャッシュを完全にクリアする必要がある場合や、計算環境を完全にリセットしたい場合に役立ちます。
# MAGIC
# MAGIC **終了**ボタンをクリックすると、クラスタを停止できます。クラスタ構成設定は維持され、**再起動**ボタンを使用して同じ構成を使用して新しいクラウドリソースを展開できます。
# MAGIC
# MAGIC **削除**ボタンをクリックすると、クラスタが停止し、クラスタ構成が削除されます。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
