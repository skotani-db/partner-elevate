# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-4603b7f5-e86f-44b2-a449-05d556e4769c
# MAGIC %md
# MAGIC # Databricks Workflowsを使用したジョブのオーケストレーション
# MAGIC
# MAGIC Databricks Jobs UIの新しいアップデートにより、複数のタスクをジョブの一部としてスケジュールする機能が追加され、Databricks Jobsはほとんどのプロダクションワークロードのオーケストレーションを完全に処理できるようになりました。
# MAGIC
# MAGIC ここでは、ノートブックタスクをトリガーとしてスタンドアロンのジョブとしてスケジュールする手順を説明し、次にDLTパイプラインを使用して依存タスクを追加します。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、次のことができるようになるはずです：
# MAGIC * Databricks Workflowジョブでノートブックタスクをスケジュールする
# MAGIC * ジョブのスケジュールオプションとクラスタータイプの違いを説明する
# MAGIC * ジョブの実行を確認して進捗状況と結果を確認する
# MAGIC * Databricks WorkflowジョブでDLTパイプラインタスクをスケジュールする
# MAGIC * Databricks Workflows UIを使用してタスク間の線形な依存関係を設定する

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-05.1.1 

# COMMAND ----------

# DBTITLE 0,--i18n-fb1b72e2-458f-4930-b070-7d60a4d3b34f
# MAGIC %md
# MAGIC
# MAGIC ## ジョブの設定を生成
# MAGIC
# MAGIC このジョブの設定には、各ユーザー固有のパラメータが必要です。
# MAGIC
# MAGIC 以下のセルを実行して、後続の手順でパイプラインを設定するために使用する値を表示します。

# COMMAND ----------

DA.print_job_config_v1()

# COMMAND ----------

# DBTITLE 0,--i18n-b3634ee0-e06e-42ca-9e70-a9a02410f705
# MAGIC %md
# MAGIC
# MAGIC ## 単一のノートブックタスクを持つジョブを設定
# MAGIC
# MAGIC Jobs UIを使用して複数のタスクを持つワークロードをオーケストレーションする場合、常に単一のタスクを持つジョブを作成してから始めます。
# MAGIC
# MAGIC 手順:
# MAGIC 1. サイドバーの「ワークフロー」ボタンをクリックし、「ジョブ」タブをクリックし、「ジョブの作成」ボタンをクリックします。
# MAGIC 2. 指定されたジョブとタスクを設定します。この手順には、上記のセルから提供された値が必要です。
# MAGIC
# MAGIC | 設定 | 指示 |
# MAGIC |--|--|
# MAGIC | タスク名 | **リセット** を入力してください |
# MAGIC | タイプ | **ノートブック** を選択してください |
# MAGIC | ソース | **ワークスペース** を選択してください |
# MAGIC | パス | ナビゲータを使用して、上記で提供された **リセットノートブックパス** を指定します |
# MAGIC | クラスタ | ドロップダウンメニューから、**既存の汎用クラスタ** の下で、クラスタを選択します |
# MAGIC | ジョブ名 | 画面の左上に、ジョブ（タスクではなく）の名前を追加するために、上記で提供された **ジョブ名** を入力します |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. 「作成」ボタンをクリックします。
# MAGIC 4. 右上の青い **今すぐ実行** ボタンをクリックしてジョブを開始します。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **注意**: オールパーパスクラスタを選択する際に、これがオールパーパスコンピュートとして請求されることに関する警告が表示されます。本番のジョブは、ワークロードに適切なサイズの新しいジョブクラスタに対してスケジュールされるべきです。これははるかに低い料金で請求されるためです。

# COMMAND ----------

DA.validate_job_v1_config()

# COMMAND ----------

# DBTITLE 0,--i18n-eb8d7811-b356-41d2-ae82-e3762add19f7
# MAGIC %md
# MAGIC
# MAGIC ## スケジューリングオプションを探る
# MAGIC 手順:
# MAGIC 1. Jobs UIの右側にある **Job Details** セクションを見つけます。
# MAGIC 1. **Trigger** セクションの下で、**Add trigger** ボタンを選択してスケジュールオプションを探します。
# MAGIC 1. **Trigger type** を **None (Manual)** から **Scheduled** に変更すると、cronスケジューリングUIが表示されます。
# MAGIC    - このUIでは、ジョブの年表スケジュール設定のための幅広いオプションを提供します。UIで設定した設定は、cron構文で出力することもでき、UIで利用できないカスタム構成が必要な場合は編集できます。
# MAGIC 1. この時点では、ジョブを **Manual** スケジュールに設定したままにしておきます。ジョブの詳細に戻るには、**キャンセル** を選択します。

# COMMAND ----------

# DBTITLE 0,--i18n-eb585218-f5df-43f8-806f-c80d6783df16
# MAGIC %md
# MAGIC
# MAGIC ## 実行を確認する
# MAGIC
# MAGIC ジョブの実行を確認するには次の手順を実行します。
# MAGIC 1. Jobsの詳細ページで、画面の左上にある**Runs**タブを選択します（現在は**Tasks**タブになっているはずです）。
# MAGIC 1. ジョブを見つけます。
# MAGIC     - ジョブが**実行中の場合**、**Active runs**セクションの下に表示されます。
# MAGIC     - ジョブが**実行が終了した場合**、**Completed runs**セクションの下に表示されます。
# MAGIC 1. **Start time**列のタイムスタンプフィールドをクリックして、出力の詳細を開きます。
# MAGIC     - ジョブが**実行中の場合**、右側のパネルに**Status**が**`Pending`**または**`Running`**で表示されるため、ノートブックの実行状態が表示されます。
# MAGIC     - ジョブが**完了した場合**、右側のパネルに**Status**が**`Succeeded`**または**`Failed`**で表示されるため、ノートブックの実行内容が表示されます。
# MAGIC   
# MAGIC このノートブックは、相対パスを使用して別のノートブックを呼び出すためにマジックコマンド **`%run`** を使用しています。このコースではカバーしていませんが、<a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank">Databricks Reposに追加された新機能を使用すると、相対パスを使用してPythonモジュールをロードできます</a>。
# MAGIC
# MAGIC スケジュールされたノートブックの実際の結果は、新しいジョブとパイプラインのために環境をリセットすることです。

# COMMAND ----------

# DBTITLE 0,--i18n-bc61c131-7d68-4633-afd7-609983e43e17
# MAGIC %md
# MAGIC ## パイプラインの生成
# MAGIC
# MAGIC このステップでは、このレッスンの最初に構成したタスクの成功後に実行するためのDLTパイプラインを追加します。
# MAGIC
# MAGIC ジョブに焦点を当て、パイプラインではなくジョブを実行するため、以下のユーティリティコマンドを使用して簡単なパイプラインを作成します。

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-19e4daea-c893-4871-8937-837970dc7c9b
# MAGIC %md
# MAGIC
# MAGIC ## DLTパイプラインタスクの構成
# MAGIC
# MAGIC 次に、このパイプラインを実行するためのタスクを追加する必要があります。
# MAGIC
# MAGIC 手順：
# MAGIC 1. ジョブの詳細ページで、**Tasks**タブをクリックします。
# MAGIC 1. 画面の中央下部にある中央に青い円形の**+**をクリックして新しいタスクを追加します。
# MAGIC 1. 次に示すようにタスクを構成します。
# MAGIC
# MAGIC | 設定 | 指示 |
# MAGIC |--|--|
# MAGIC | タスク名 | **DLT**を入力します |
# MAGIC | タイプ | **Delta Live Tables pipeline**を選択します |
# MAGIC | パイプライン | 上記で構成したDLTパイプラインを選択します |
# MAGIC | 依存タスク | 前に定義した**Reset**を選択します |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. 青い**Create task**ボタンをクリックします
# MAGIC     - これで、2つのボックスとそれらの間にある下向きの矢印を持つ画面が表示されるはずです。
# MAGIC     - **`Reset`** タスクは上部にあり、 **`DLT`** タスクにつながっています。
# MAGIC     - この可視化は、これらのタスク間の依存関係を表しています。
# MAGIC 5. 下記のコマンドを実行して構成を検証します。
# MAGIC     - エラーが報告された場合、次の手順を繰り返してすべてのエラーを削除してください。
# MAGIC       - エラー（複数の場合はすべてのエラー）を修正します。
# MAGIC       - **Create task**ボタンをクリックします。
# MAGIC       - 構成を検証します。

# COMMAND ----------

DA.validate_job_v2_config()

# COMMAND ----------

# DBTITLE 0,--i18n-1c949168-e917-455d-8a54-2768592a16f1
# MAGIC %md
# MAGIC
# MAGIC ## ジョブを実行する
# MAGIC ジョブが正しく構成されたら、ジョブを開始するために右上の青い**Run now**ボタンをクリックします。
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **注意**: すべての用途のクラスタを選択すると、すべての目的の計算として請求される警告が表示されます。本番のジョブは常に、ワークロードに適切なサイズの新しいジョブクラスタに対してスケジュールする必要があります。これにより、低料金で請求されます。
# MAGIC
# MAGIC **注意**: ジョブおよびパイプラインのインフラストラクチャの展開に数分待つ必要がある場合があります。

# COMMAND ----------

# DBTITLE 0,--i18n-666a45d2-1a19-45ba-b771-b47456e6f7e4
# MAGIC %md
# MAGIC
# MAGIC ## マルチタスク実行結果の確認
# MAGIC
# MAGIC 実行結果を確認するには、次の手順を実行します。
# MAGIC 1. ジョブ詳細ページで、**Runs**タブを再度選択し、最新の実行を**Active runs**または**Completed runs**の下で選択します。ジョブが完了しているかどうかに応じて異なります。
# MAGIC     - タスクの可視化はリアルタイムで更新され、実行中のタスクがどれかを示し、タスクの失敗が発生すると色が変わります。
# MAGIC 1. タスクボックスをクリックすると、UIでスケジュールされたノートブックが表示されます。
# MAGIC     - これは、前のDatabricks Jobs UIの上に追加のオーケストレーションのレイヤーと考えることができます。
# MAGIC     - ジョブをCLIまたはREST APIを使用してスケジュールしている場合、[ジョブを構成および結果を取得するためのJSON構造](https://docs.databricks.com/dev-tools/api/latest/jobs.html)はUIと同様の更新を受けています。
# MAGIC
# MAGIC **注意**: 現時点では、タスクとしてスケジュールされたDLTパイプラインは、実行のGUIで直接結果を表示しません。代わりに、スケジュールされたパイプラインのためにDLTパイプラインGUIに戻されます。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
