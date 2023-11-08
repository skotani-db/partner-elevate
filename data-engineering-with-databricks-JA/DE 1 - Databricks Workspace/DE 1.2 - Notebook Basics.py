# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-2d4e57a0-a2d5-4fad-80eb-98ff30d09a37
# MAGIC %md
# MAGIC
# MAGIC # ノートブックの基本
# MAGIC
# MAGIC ノートブックは、Databricksでコードを対話的に開発および実行する主要な手段です。このレッスンでは、Databricksノートブックの使用方法について基本的な紹介を提供します。
# MAGIC
# MAGIC Databricksノートブックを以前に使用したことがある場合、これがDatabricks Reposでノートブックを実行する初めての場合でも、基本的な機能は同じであることに気付くでしょう。次のレッスンでは、Databricks Reposがノートブックに追加する機能のいくつかを確認します。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの最後までに、以下のことができるようになるはずです。
# MAGIC * ノートブックをクラスタにアタッチする
# MAGIC * ノートブックでセルを実行する
# MAGIC * ノートブックの言語を設定する
# MAGIC * マジックコマンドを説明し、使用する
# MAGIC * SQLセルを作成および実行する
# MAGIC * Pythonセルを作成および実行する
# MAGIC * マークダウンセルを作成する
# MAGIC * Databricksノートブックをエクスポートする
# MAGIC * Databricksノートブックのコレクションをエクスポートする

# COMMAND ----------

# DBTITLE 0,--i18n-e7c4cc85-5ab7-46c1-9da3-f9181d77d118
# MAGIC %md
# MAGIC
# MAGIC ## クラスタにアタッチ
# MAGIC
# MAGIC 前のレッスンで、クラスタをデプロイしたか、管理者が設定したクラスタを特定するはずです。
# MAGIC
# MAGIC 画面の右上隅にあるクラスタセレクター（"接続"ボタン）をクリックし、ドロップダウンメニューからクラスタを選択します。ノートブックがクラスタに接続されている場合、このボタンにはクラスタの名前が表示されます。
# MAGIC
# MAGIC **注意**: クラスタのデプロイには数分かかることがあります。リソースがデプロイされたら、クラスタ名の左側に実線の緑の円が表示されます。クラスタに左側に空の灰色の円がある場合は、クラスタを起動するための<a href="https://docs.databricks.com/clusters/clusters-manage.html#start-a-cluster" target="_blank">クラスタの起動手順</a>に従う必要があります。

# COMMAND ----------

# DBTITLE 0,--i18n-23aed87f-d375-4b81-8c3c-d4375cda0384
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ## ノートブックの基本
# MAGIC
# MAGIC ノートブックはセルごとのコードの実行を提供します。ノートブックでは複数の言語を混在させることができます。ユーザーはコードを強化するためにプロット、画像、およびマークダウンテキストを追加できます。
# MAGIC
# MAGIC このコース全体で、私たちのノートブックは学習用のツールとして設計されています。ノートブックはDatabricksで本番コードとして簡単に展開できるだけでなく、データの探索、レポート作成、ダッシュボード作成のための強力なツールセットも提供します。
# MAGIC
# MAGIC ### セルの実行
# MAGIC * 以下のセルを実行するには、次のいずれかのオプションを使用します：
# MAGIC   * **CTRL+ENTER**または**CTRL+RETURN**
# MAGIC   * セルを実行して次のセルに移動するために**SHIFT+ENTER**または**SHIFT+RETURN**を使用します
# MAGIC   * ここで表示されているように**Run Cell**、**Run All Above**、または**Run All Below**を使用します<br/><img style="box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png"/>

# COMMAND ----------

print("I'm running Python!")

# COMMAND ----------

# DBTITLE 0,--i18n-5b14b4c2-c009-4786-8058-a3ddb61fa41d
# MAGIC %md
# MAGIC
# MAGIC **注意**: セルごとのコードの実行は、セルが複数回実行されたり、順序が逆になったりすることを意味します。明示的に指示されていない限り、このコースのノートブックは常に上から下に向けて一つのセルずつ実行されることを前提としてください。エラーが発生した場合、トラブルシューティングを試みる前にセルの前後のテキストを読み、エラーが意図的な学習の瞬間でないか確認してください。ほとんどのエラーは、ノートブック内で見落とされた以前のセルを実行するか、トップから全体のノートブックを再実行することで解決できます。

# COMMAND ----------

# DBTITLE 0,--i18n-9be4ac54-8411-45a0-ad77-7173ec7402f8
# MAGIC %md
# MAGIC
# MAGIC ### デフォルトのノートブック言語の設定
# MAGIC
# MAGIC 上記のセルは、ノートブックのデフォルト言語がPythonに設定されているため、Pythonコマンドを実行します。
# MAGIC
# MAGIC DatabricksノートブックはPython、SQL、Scala、およびRをサポートしています。ノートブックの作成時に言語を選択できますが、いつでも変更できます。
# MAGIC
# MAGIC デフォルトの言語は、ページの上部にあるノートブックのタイトルの右側に表示されます。このコースでは、SQLとPythonのノートブックを組み合わせて使用します。
# MAGIC
# MAGIC このノートブックのデフォルト言語をSQLに変更します。
# MAGIC
# MAGIC 手順:
# MAGIC * 画面上部のノートブックタイトルの横にある **Python** をクリックします
# MAGIC * ポップアップ画面で、ドロップダウンリストから **SQL** を選択します
# MAGIC
# MAGIC **注意**: ちょうどこのセルの前に、新しい行が <strong><code>&#37;python</code></strong> と表示されるはずです。これについては後で説明します。

# COMMAND ----------

# DBTITLE 0,--i18n-3185e9b5-fcba-40aa-916b-5f3daa555cf5
# MAGIC %md
# MAGIC
# MAGIC ### SQLセルを作成して実行する
# MAGIC
# MAGIC * このセルをハイライトし、キーボードの **B** ボタンを押して、下に新しいセルを作成します
# MAGIC * 次のコードを下のセルにコピーしてから、セルを実行します
# MAGIC
# MAGIC **`%sql`**<br/>
# MAGIC **`SELECT "SQLを実行しています！"`**
# MAGIC
# MAGIC **注意**: セルの追加、移動、削除にはさまざまな方法があり、GUIオプションやキーボードショートカットが含まれます。詳細については、<a href="https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks" target="_blank">ドキュメント</a>を参照してください。

# COMMAND ----------

# DBTITLE 0,--i18n-5046f81c-cdbf-42c3-9b39-3be0721d837e
# MAGIC %md
# MAGIC
# MAGIC ## マジックコマンド
# MAGIC * マジックコマンドは、Databricksノートブックに固有のものです
# MAGIC * 他のノートブック製品で見られるマジックコマンドに似ています
# MAGIC * これらは、ノートブックの言語に関係なく同じ結果を提供する組み込みコマンドです
# MAGIC * セルの先頭に単一のパーセント（%）記号があると、それはマジックコマンドを識別します
# MAGIC   * セルごとに1つのマジックコマンドしか使用できません
# MAGIC   * マジックコマンドはセル内で最初に指定する必要があります

# COMMAND ----------

# DBTITLE 0,--i18n-39d2c50e-4b92-46ef-968c-f358114685be
# MAGIC %md
# MAGIC
# MAGIC ### 言語マジック
# MAGIC 言語マジックコマンドは、ノートブックのデフォルト言語以外の言語でコードを実行できるようにします。このコースでは、以下の言語マジックを見ていきます：
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC
# MAGIC 現在設定されているノートブックのタイプに対して言語マジックを追加する必要はありません。
# MAGIC
# MAGIC 上記でノートブックの言語をPythonからSQLに変更した際、既存のPythonで書かれたセルには<strong><code>&#37;python</code></strong>コマンドが追加されました。
# MAGIC
# MAGIC **注意**: ノートブックのデフォルト言語を頻繁に変更する代わりに、デフォルトの主要言語を維持し、他の言語でコードを実行する必要がある場合にのみ言語マジックを使用すべきです。

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Hello SQL!"

# COMMAND ----------

# DBTITLE 0,--i18n-94da1696-d0cf-418f-ba5a-d105a5ecdaac
# MAGIC %md
# MAGIC
# MAGIC ### Markdown
# MAGIC
# MAGIC マジックコマンド **&percnt;md** は、セル内でマークダウンを表示することを可能にします：
# MAGIC * このセルをダブルクリックして編集を開始します
# MAGIC * 編集を終了するには **`Esc`** キーを押します
# MAGIC
# MAGIC # タイトル1
# MAGIC ## タイトル2
# MAGIC ### タイトル3
# MAGIC
# MAGIC これは緊急放送システムのテストです。これはテストです。
# MAGIC
# MAGIC これは **太字** の単語を含むテキストです。
# MAGIC
# MAGIC これは *斜体* の単語を含むテキストです。
# MAGIC
# MAGIC これは順序付きリストです
# MAGIC 1. 1つ
# MAGIC 1. 2つ
# MAGIC 1. 3つ
# MAGIC
# MAGIC これは順不同リストです
# MAGIC * りんご
# MAGIC * 桃
# MAGIC * バナナ
# MAGIC
# MAGIC リンク/埋め込みHTML： <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipedia</a>
# MAGIC
# MAGIC 画像：
# MAGIC ![Spark Engines](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC
# MAGIC そしてもちろん、テーブルもあります：
# MAGIC
# MAGIC | 名前   | 値    |
# MAGIC |--------|-------|
# MAGIC | Yi     | 1     |
# MAGIC | Ali    | 2     |
# MAGIC | Selina | 3     |

# COMMAND ----------

# DBTITLE 0,--i18n-537e86bc-782f-4167-9899-edb3bd2b9e38
# MAGIC %md
# MAGIC
# MAGIC ### %run
# MAGIC * 別のノートブックから **%run** マジックコマンドを使用してノートブックを実行できます
# MAGIC * 実行するノートブックは相対パスで指定されます
# MAGIC * 参照されたノートブックは、呼び出し元のノートブックから実行されるため、一時的なビューやその他のローカルな宣言は呼び出し元のノートブックから利用できます

# COMMAND ----------

# DBTITLE 0,--i18n-d5c27671-b3c8-4b8c-a559-40cf7988f92f
# MAGIC %md
# MAGIC
# MAGIC 以下のセルのコメントを外して実行すると、次のエラーが発生します:<br/>
# MAGIC **`Error in SQL statement: AnalysisException: Table or view not found: demo_tmp_vw`**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# DBTITLE 0,--i18n-d0df4a17-abb4-42d3-ba37-c9a78f4fc9c0
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ただし、このセルを実行することで、それと他のいくつかの変数と関数を宣言できます。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01.2

# COMMAND ----------

# DBTITLE 0,--i18n-6a001755-5259-4fef-a5d3-2661d5301237
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC **`../Includes/Classroom-Setup-01.2`** ノートブックには、スキーマを作成して **`USE`** するロジックや、テンポラリビュー **`demo_temp_vw`** を作成するロジックが含まれています。
# MAGIC
# MAGIC このテンポラリビューが現在のノートブックセッションで利用可能であることを、以下のクエリで確認できます。

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# DBTITLE 0,--i18n-c28ecc03-8919-488f-bce7-e2fc0a451870
# MAGIC %md
# MAGIC
# MAGIC このコース全体で、レッスンやラボの環境を設定するのに役立つ "セットアップ" ノートブックのパターンを使用します。
# MAGIC
# MAGIC これらの "提供された" 変数、関数、およびその他のオブジェクトは、 **`DBAcademyHelper`** のインスタンスである **`DA`** オブジェクトの一部であるため、簡単に識別できるはずです。
# MAGIC
# MAGIC これを考慮に入れると、ほとんどのレッスンでは、ユーザー名から派生した変数を使用してファイルやスキーマを整理します。
# MAGIC
# MAGIC このパターンを使用することで、共有ワークスペースで他のユーザーとの衝突を回避できます。
# MAGIC
# MAGIC 以下のセルでは、このノートブックのセットアップスクリプトで事前に定義されたいくつかの変数を表示するためにPythonを使用しています。

# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.schema_name:       {DA.schema_name}")

# COMMAND ----------

# DBTITLE 0,--i18n-1145175f-c51e-4cf5-a4a5-b0e3290a73a2
# MAGIC %md
# MAGIC
# MAGIC さらに、これらの同じ変数はSQLコンテキストに「インジェクト」されており、SQLステートメントで使用できます。
# MAGIC
# MAGIC これについては後で詳しく説明しますが、次のセルの例でその一例を見ることができます。
# MAGIC
# MAGIC ![アイコン](https://files.training.databricks.com/images/icon_note_32.png) これらの2つの例で単語 **`da`** と **`DA`** のキャスィングの微妙な違いに注意してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.username}' AS current_username,
# MAGIC        '${da.paths.working_dir}' AS working_directory,
# MAGIC        '${da.schema_name}' as schema_name

# COMMAND ----------

# DBTITLE 0,--i18n-8330ad3c-8d48-42fa-9b6f-72e818426ed4
# MAGIC %md
# MAGIC
# MAGIC ## Databricks ユーティリティ
# MAGIC Databricks ノートブックには、環境を設定および操作するためのさまざまなユーティリティコマンドを提供する **`dbutils`** オブジェクトが含まれています。[dbutils ドキュメント](https://docs.databricks.com/user-guide/dev-tools/dbutils.html)
# MAGIC
# MAGIC このコースでは、時折 Python セルからファイルのディレクトリをリストアップするために **`dbutils.fs.ls()`** を使用します。

# COMMAND ----------

path = f"{DA.paths.datasets}"
dbutils.fs.ls(path)

# COMMAND ----------

# DBTITLE 0,--i18n-25eb75f8-6e75-41a6-a304-d140844fa3e6
# MAGIC %md
# MAGIC
# MAGIC ## display()
# MAGIC
# MAGIC When running SQL queries from cells, results will always be displayed in a rendered tabular format.
# MAGIC
# MAGIC When we have tabular data returned by a Python cell, we can call **`display`** to get the same type of preview.
# MAGIC
# MAGIC Here, we'll wrap the previous list command on our file system with **`display`**.

# COMMAND ----------

path = f"{DA.paths.datasets}"
files = dbutils.fs.ls(path)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-42b624ff-8cc9-4318-bedc-e3b340cb1b81
# MAGIC %md
# MAGIC
# MAGIC **`display()`** コマンドには以下の機能と制限があります。
# MAGIC * 結果のプレビューは1000レコードまでに制限されます
# MAGIC * 結果データをCSVとしてダウンロードするためのボタンを提供します
# MAGIC * プロットのレンダリングを許可します

# COMMAND ----------

# DBTITLE 0,--i18n-c7f02dde-9b21-4edb-bded-5ce0eed56d03
# MAGIC %md
# MAGIC
# MAGIC ## ノートブックのダウンロード
# MAGIC
# MAGIC 個々のノートブックやノートブックのコレクションをダウンロードするためのいくつかのオプションがあります。
# MAGIC
# MAGIC ここでは、このノートブックとこのコースのすべてのノートブックのコレクションをダウンロードするプロセスを進めます。
# MAGIC
# MAGIC ### ノートブックのダウンロード
# MAGIC
# MAGIC 手順：
# MAGIC * ノートブックの左上にある、**ファイル** オプションをクリックします
# MAGIC * 表示されるメニューで、**エクスポート** の上にカーソルを合わせ、**ソースファイル** を選択します
# MAGIC
# MAGIC ノートブックは個人のラップトップにダウンロードされます。現在のノートブック名で命名され、デフォルトの言語のファイル拡張子が付けられます。これらのソースファイルは任意のファイルエディタで開くことができ、Databricksノートブックの生の内容を表示できます。
# MAGIC
# MAGIC これらのソースファイルは、任意のDatabricksワークスペースにアップロードできます。
# MAGIC
# MAGIC ### ノートブックのコレクションをダウンロード
# MAGIC
# MAGIC **注意**: 以下の手順は、これらの資料を **Repos** を使用してインポートしたことを前提としています。
# MAGIC
# MAGIC 手順：
# MAGIC * 左側のサイドバーにある ![](https://files.training.databricks.com/images/repos-icon.png) **Repos** をクリックします
# MAGIC   * これにより、このノートブックの親ディレクトリのプレビューが表示されます
# MAGIC * 画面の中央あたりに、ディレクトリプレビューの左側に左矢印があるはずです。これをクリックしてファイル階層を上に移動します。
# MAGIC * **Data Engineer Learning Path** というディレクトリが表示されるはずです。メニューを表示するために、ディレクトリプレビューの左側にある下向き矢印/シェブロンをクリックします
# MAGIC * メニューから、**DBCアーカイブ** を選択します
# MAGIC
# MAGIC ダウンロードされるDBC（Databricks Cloud）ファイルには、このコースのディレクトリとノートブックの圧縮されたコレクションが含まれています。ユーザーはこれらのDBCファイルをローカルで編集しようとしないでくださいが、ノートブックの内容を移動または共有するために、安全に任意のDatabricksワークスペースにアップロードできます。
# MAGIC
# MAGIC **注意**: ノートブックのコレクションをダウンロードする際、結果のプレビューやプロットもエクスポートされます。ソースノートブックをダウンロードする場合、コードのみが保存されます。

# COMMAND ----------

# DBTITLE 0,--i18n-30e63e01-ca85-461a-b980-ea401904731f
# MAGIC %md
# MAGIC
# MAGIC ## もっと学ぶ
# MAGIC
# MAGIC Databricksプラットフォームとノートブックのさまざまな機能について詳しく学ぶために、ドキュメンテーションを探索することをお勧めします。
# MAGIC * <a href="https://docs.databricks.com/user-guide/index.html#user-guide" target="_blank">ユーザーガイド</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Databricks入門</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/notebooks/index.html" target="_blank">ユーザーガイド / ノートブック</a>
# MAGIC * <a href="https://docs.databricks.com/notebooks/notebooks-manage.html#notebook-external-formats" target="_blank">ノートブックのインポート - サポートされている形式</a>
# MAGIC * <a href="https://docs.databricks.com/repos/index.html" target="_blank">Repos</a>
# MAGIC * <a href="https://docs.databricks.com/administration-guide/index.html#administration-guide" target="_blank">管理ガイド</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">クラスタの設定</a>
# MAGIC * <a href="https://docs.databricks.com/api/latest/index.html#rest-api-2-0" target="_blank">REST API</a>
# MAGIC * <a href="https://docs.databricks.com/release-notes/index.html#release-notes" target="_blank">リリースノート</a>

# COMMAND ----------

# DBTITLE 0,--i18n-9987fd58-1023-4dbd-8319-40332f909181
# MAGIC %md
# MAGIC
# MAGIC ## さらに一つ注意点！
# MAGIC
# MAGIC 各レッスンの最後に、以下のコマンド **`DA.cleanup()`** が表示されます。
# MAGIC
# MAGIC このメソッドは、レッスン固有のスキーマと作業ディレクトリを削除し、ワークスペースを清潔に保ち、各レッスンの不変性を維持しようとするものです。
# MAGIC
# MAGIC このレッスンに関連するテーブルとファイルを削除するために、以下のセルを実行してください。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
