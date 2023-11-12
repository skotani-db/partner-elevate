# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-da41af42-59a3-42d8-af6d-4ab96146397c
# MAGIC %md
# MAGIC # Delta Live Tables UIの使用
# MAGIC
# MAGIC このデモでは、DLT UIを探索します。このレッスンの終わりまでに、次のことができるようになります：
# MAGIC
# MAGIC * DLTパイプラインをデプロイする
# MAGIC * 結果のDAGを探索する
# MAGIC * パイプラインの更新を実行する

# COMMAND ----------

# DBTITLE 0,--i18n-d84e8f59-6cda-4c81-8547-132eb20b48b2
# MAGIC %md
# MAGIC ## クラスルームのセットアップ
# MAGIC
# MAGIC このコースの作業環境を構成するには、次のセルを実行してください。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.1

# COMMAND ----------

# DBTITLE 0,--i18n-ba2a4dfe-ca17-4070-b35a-37068ff9c51d
# MAGIC %md
# MAGIC
# MAGIC ## パイプラインの構成生成
# MAGIC このパイプラインの構成には、特定のユーザーに固有のパラメータが必要です。
# MAGIC
# MAGIC 以下のコードセルで、使用する言語を指定するには、適切な行のコメントを解除します。
# MAGIC
# MAGIC その後、セルを実行して、次の手順でパイプラインを構成するために使用する値を出力します。

# COMMAND ----------

pipeline_language = "SQL"
#pipeline_language = "Python"

DA.print_pipeline_config(pipeline_language)

# COMMAND ----------

# DBTITLE 0,--i18n-bc4e7bc9-67e1-4393-a3c5-79a1f585cdc8
# MAGIC %md
# MAGIC このレッスンでは、セルの出力でノートブック＃1として指定された単一のノートブックでパイプラインを展開します。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> **ヒント:** 後のレッスンでパイプラインにノートブック＃2と＃3を追加する際に、上記のパスを参照することがお勧めです。

# COMMAND ----------

# DBTITLE 0,--i18n-9b609cc5-91c8-4213-b6f7-1c737a6e44a3
# MAGIC %md
# MAGIC ## パイプラインの作成と構成
# MAGIC
# MAGIC まず、単一のノートブック（ノートブック＃1）を使用してパイプラインを作成します。
# MAGIC
# MAGIC 手順：
# MAGIC 1. サイドバーの **Workflows** ボタンをクリックし、 **Delta Live Tables** タブをクリックし、 **Create Pipeline** をクリックします。
# MAGIC 2. 以下の指示に従ってパイプラインを構成します。この手順では、上記のセル出力で提供された値が必要です。
# MAGIC
# MAGIC | 設定 | 指示 |
# MAGIC |--|--|
# MAGIC | パイプライン名 | 上記の **Pipeline Name** を入力します |
# MAGIC | 製品版 | **Advanced** を選択します |
# MAGIC | パイプラインモード | **Triggered** を選択します |
# MAGIC | クラスターポリシー | 上記の **Policy** を選択します |
# MAGIC | ノートブックライブラリ | ナビゲーターを使用して、上記の **Notebook # 1 Path** を選択または入力します |
# MAGIC | ストレージの場所 | 上記の **Storage Location** を入力します |
# MAGIC | ターゲットスキーマ | 上記の **Target** データベース名を入力します |
# MAGIC | クラスターモード | クラスターの自動スケーリングを無効にするには、 **Fixed size** を選択します |
# MAGIC | ワーカー | シングルノードクラスターを使用するには、 **0** を入力します |
# MAGIC | Photon Acceleration | これを有効にするには、このチェックボックスをオンにします |
# MAGIC | 構成 | 追加の設定を表示するには、 **Advanced** をクリックします。<br>表の下の行＃1に **Key** と **Value** を入力するには、 **Add Configuration** をクリックします。<br>表の下の行＃2に **Key** と **Value** を入力するには、 **Add Configuration** をクリックします |
# MAGIC | チャネル | **Current** を選択して、現在のランタイムバージョンを使用します |
# MAGIC
# MAGIC | 構成 | キー | 値 |
# MAGIC | ------------- | ------------------- | ------------------------------------------ |
# MAGIC | ＃1 | **`spark.master`** | **`local[*]`** |
# MAGIC | ＃2 | **`source`** | 上記の **source** を入力します |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. **Create** ボタンをクリックします。
# MAGIC 4. パイプラインモードが **Development** に設定されていることを確認します。

# COMMAND ----------

DA.validate_pipeline_config(pipeline_language)

# COMMAND ----------

# DBTITLE 0,--i18n-d8e19679-0c2f-48cc-bc80-5f1243ff94c8
# MAGIC %md
# MAGIC #### パイプライン構成に関する追加の注意事項
# MAGIC 上記のパイプライン設定に関するいくつかの注意事項です。
# MAGIC
# MAGIC - **Pipeline mode** - これはパイプラインがどのように実行されるかを指定します。モードは、レイテンシとコストの要件に基づいて選択します。
# MAGIC   - `Triggered` パイプラインは一度実行され、次の手動またはスケジュールされた更新までシャットダウンします。
# MAGIC   - `Continuous` パイプラインは継続的に実行され、新しいデータが到着するたびにデータを取り込みます。
# MAGIC - **Notebook libraries** - この文書は標準の Databricks ノートブックですが、SQL 構文は DLT テーブルの宣言に特化しています。この構文については、以下の演習で詳しく説明します。
# MAGIC - **Storage location** - このオプションのフィールドを使用すると、パイプラインの実行に関連するログ、テーブルなどを保存する場所を指定できます。指定しない場合、DLT は自動的にディレクトリを生成します。
# MAGIC - **Target** - このオプションのフィールドが指定されていない場合、テーブルはメタストアに登録されませんが、DBFS で利用できます。このオプションに関する詳細な情報については、<a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#publish-tables" target="_blank">ドキュメント</a>を参照してください。
# MAGIC - **Cluster mode**、**Min Workers**、**Max Workers** - これらのフィールドは、パイプラインを処理する基本クラスターのワーカー構成を制御します。ここでは、ワーカーの数を 0 に設定しています。これは上記で定義した **spark.master** パラメータと連動して、クラスターをシングルノードクラスターとして構成します。
# MAGIC - **source** - これらのキーは大文字小文字を区別します。単語 "source" はすべて小文字であることを確認してください！
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-6f8d9d42-99e2-40a5-b80e-a6e6fedd7279
# MAGIC %md
# MAGIC ## パイプラインの実行
# MAGIC
# MAGIC パイプラインが作成されたら、次にそのパイプラインを実行します。
# MAGIC
# MAGIC 1. **Development** を選択して、パイプラインを開発モードで実行します。開発モードでは、クラスターを再利用して（各実行ごとに新しいクラスターを作成するのではなく）、エラーをすぐに特定して修正できるように、リトライを無効にしています。この機能についての詳細は、<a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank">ドキュメント</a>を参照してください。
# MAGIC 2. **Start** をクリックします。
# MAGIC
# MAGIC 初回の実行はクラスターがプロビジョニングされるため、数分かかります。2回目以降の実行はかなり速くなります。
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-75d0f6d5-17c6-419e-aacf-be7560f394b6
# MAGIC %md
# MAGIC ## DAGの探索
# MAGIC
# MAGIC パイプラインが完了すると、実行フローがグラフで表示されます。
# MAGIC
# MAGIC テーブルを選択して詳細を確認します。
# MAGIC
# MAGIC **orders_silver** を選択します。**Data Quality** セクションで報告されている結果に注意してください。
# MAGIC
# MAGIC トリガーされる更新ごとに、すべての新しく到着したデータがパイプラインを通過します。メトリクスは常に現在の実行に対して報告されます。
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-4cef0694-c05f-44ba-84bf-cd14a63eda17
# MAGIC %md
# MAGIC ## 別のバッチのデータを着陸させる
# MAGIC
# MAGIC 以下のセルを実行して、ソースディレクトリにさらにデータを着陸させ、その後手動でパイプラインの更新をトリガーします。

# COMMAND ----------

DA.dlt_data_factory.load()

# COMMAND ----------

# DBTITLE 0,--i18n-58129206-f245-419e-b51e-b126376a9a45
# MAGIC %md
# MAGIC コースを進める中で、このノートブックに戻り、上記の手順を使用して新しいデータをロードすることができます。
# MAGIC このノートブック全体を再実行すると、ソースデータとDLTパイプラインの基になるデータファイルが削除されます。
# MAGIC データを削除せずに新しいデータをロードしたい場合や、クラスタから切断された場合などは、<a href="$./DE 4.99 - Land New Data" target="_blank">DE 4.99 - Land New Data</a> ノートブックを参照してください。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
