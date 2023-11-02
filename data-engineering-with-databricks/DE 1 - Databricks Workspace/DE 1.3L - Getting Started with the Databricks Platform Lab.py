# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-4212c527-b1c1-4cce-b629-b5ffb5c57d68
# MAGIC %md
# MAGIC
# MAGIC # Databricks プラットフォームのはじめ方
# MAGIC
# MAGIC このノートブックは、Databricks Data Science and Engineering Workspace の基本機能のいくつかを実際に試してみるためのハンズオンレビューを提供します。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このラボの最後までに、以下のことができるようになるはずです：
# MAGIC - ノートブックの名前を変更し、デフォルトの言語を変更する
# MAGIC - クラスタをアタッチする
# MAGIC - **`%run`** マジックコマンドを使用する
# MAGIC - Python および SQL のセルを実行する
# MAGIC - マークダウンセルを作成する

# COMMAND ----------

# DBTITLE 0,--i18n-eb166a05-9a22-4a74-b54e-3f9e5779f342
# MAGIC %md
# MAGIC
# MAGIC # ノートブックの名前を変更する
# MAGIC
# MAGIC ノートブックの名前を変更するのは簡単です。このページの上部にある名前をクリックし、名前を変更します。後でこのノートブックに戻りやすくするために、既存の名前の最後に短いテスト文字列を追加してください。

# COMMAND ----------

# DBTITLE 0,--i18n-a975b60c-9871-4736-9b4f-194577d730f0
# MAGIC %md
# MAGIC
# MAGIC # クラスタをアタッチする
# MAGIC
# MAGIC ノートブックでセルを実行するには、計算リソースが必要であり、これはクラスタによって提供されます。ノートブックでセルを初めて実行すると、クラスタがまだ添付されていない場合、クラスタをアタッチするように求められます。
# MAGIC
# MAGIC クラスタを添付するには、このページの右上近くにあるドロップダウンをクリックし、以前に作成したクラスタを選択してください。これにより、ノートブックの実行状態がクリアされ、ノートブックが選択したクラスタに接続されます。
# MAGIC
# MAGIC なお、ドロップダウンメニューには、必要に応じてクラスタを起動または再起動するオプションも用意されています。また、クラスタを簡単に取り外して再度添付することもできます。これは、必要なときに実行状態をクリアするために便利です。

# COMMAND ----------

# DBTITLE 0,--i18n-4cd4b089-e782-4e81-9b88-5c0abd02d03f
# MAGIC %md
# MAGIC
# MAGIC # %run の使用
# MAGIC
# MAGIC あらゆるタイプの複雑なプロジェクトは、それらをより単純で再利用可能なコンポーネントに分解する能力から利益を得ることができます。
# MAGIC
# MAGIC Databricksノートブックのコンテキストでは、この機能は **`%run`** マジックコマンドを介して提供されます。
# MAGIC
# MAGIC この方法で使用すると、変数、関数、およびコードブロックは現在のプログラムコンテキストの一部になります。
# MAGIC
# MAGIC 次の例を考えてみてください。
# MAGIC
# MAGIC **`Notebook_A`** には次の4つのコマンドがあります：
# MAGIC   1. **`name = "John"`**
# MAGIC   2. **`print(f"Hello {name}")`**
# MAGIC   3. **`%run ./Notebook_B`**
# MAGIC   4. **`print(f"Welcome back {full_name}`**
# MAGIC
# MAGIC **`Notebook_B`** には1つのコマンドしかありません：
# MAGIC   1. **`full_name = f"{name} Doe"`**
# MAGIC
# MAGIC **`Notebook_B`** を実行しようとすると、**`Notebook_B`** 内で定義されていない変数 **`name`** が存在するため、実行に失敗します。
# MAGIC
# MAGIC 同様に、**`Notebook_A`** では **`full_name`** という変数を使用しており、これは **`Notebook_A`** では定義されていないため、失敗すると考えるかもしれませんが、実際には失敗しません！
# MAGIC
# MAGIC 実際に起こることは、2つのノートブックが結合され、以下のように実行されることです：
# MAGIC 1. **`name = "John"`**
# MAGIC 2. **`print(f"Hello {name}")`**
# MAGIC 3. **`full_name = f"{name} Doe"`**
# MAGIC 4. **`print(f"Welcome back {full_name}")`**
# MAGIC
# MAGIC したがって、期待どおりの動作を提供します：
# MAGIC * **`Hello John`**
# MAGIC * **`Welcome back John Doe`**

# COMMAND ----------

# DBTITLE 0,--i18n-40ca42ab-4275-4d92-b151-995429e54486
# MAGIC %md
# MAGIC
# MAGIC このノートブックを含むフォルダには、 **`ExampleSetupFolder`** というサブフォルダが含まれており、このサブフォルダには **`example-setup`** という名前のノートブックが含まれています。
# MAGIC
# MAGIC このシンプルなノートブックは変数 **`my_name`** を宣言し、それを **`None`** に設定し、次に **`example_df`** という名前の DataFrame を作成します。
# MAGIC
# MAGIC **`example-setup`** ノートブックを開き、 **`my_name`** を **`None`** ではなく、代わりに名前（または誰かの名前）を引用符で囲んで設定し、次の2つのセルが **`AssertionError`** をスローせずに実行されるように変更してください。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> この演習では、コースウェアを設定するために使用される **`_utility-methods`** と **`DBAcademyHelper`** という追加の参照が表示されますが、この演習では無視してください。

# COMMAND ----------

# MAGIC %run ./ExampleSetupFolder/example-setup

# COMMAND ----------

assert my_name is not None, "Name is still None"
print(my_name)

# COMMAND ----------

# DBTITLE 0,--i18n-e5ef8dff-bfa6-4f9e-8ad3-d5ef322b978d
# MAGIC %md
# MAGIC
# MAGIC ## Pythonセルを実行する
# MAGIC
# MAGIC **`example-setup`** ノートブックが実行されたことを確認するために、次のセルを実行して **`example_df`** DataFrame を表示してください。このテーブルは、増加する値の16行から構成されています。

# COMMAND ----------

display(example_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6cb46bcc-9797-4782-931c-a7b8350146b2
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # 言語の変更
# MAGIC
# MAGIC デフォルトの言語がPythonに設定されていることに気付いていますね。ノートブック名の右側にある **Python** ボタンをクリックして、デフォルトの言語をSQLに変更してください。
# MAGIC
# MAGIC Pythonセルは自動的に <strong><code>&#37;python</code></strong> マジックコマンドで前置され、これによりセルの有効性が維持されることに注意してください。また、この操作により実行状態もクリアされます。

# COMMAND ----------

# DBTITLE 0,--i18n-478faa69-6814-4725-803b-3414a1a803ae
# MAGIC %md
# MAGIC
# MAGIC # マークダウンセルの作成
# MAGIC
# MAGIC このセルの下に新しいセルを追加してください。少なくとも以下の要素を含むマークダウンを記入してください。
# MAGIC
# MAGIC * ヘッダー
# MAGIC * 箇条書き
# MAGIC * リンク（HTMLまたはマークダウン規則を使用して選択）

# COMMAND ----------

# DBTITLE 0,--i18n-55b2a6c6-2fc6-4c57-8d6d-94bba244d86e
# MAGIC %md
# MAGIC
# MAGIC ## SQLセルを実行
# MAGIC
# MAGIC 以下のセルを実行して、SQLを使用してDeltaテーブルをクエリします。これにより、すべてのDBFSインストールに含まれるDatabricksが提供するサンプルデータセットでバックアップされたテーブルに対する単純なクエリが実行されます。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.datasets}/nyctaxi-with-zipcodes/data`

# COMMAND ----------

# DBTITLE 0,--i18n-9993ed50-d8cf-4f37-bc76-6b18789447d6
# MAGIC %md
# MAGIC
# MAGIC 以下のセルを実行して、このテーブルのバックアップファイルを表示します。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-c31a3318-a114-46e8-a744-18e8f8aa071e
# MAGIC %md
# MAGIC
# MAGIC # ノートブックの状態をクリアする
# MAGIC
# MAGIC 時々、ノートブックで定義されたすべての変数をクリアし、最初からやり直すことは役立ちます。これはセルを個別にテストしたい場合や、単に実行状態をリセットしたい場合に役立ちます。
# MAGIC
# MAGIC **Run** メニューに移動し、**Clear state and outputs** を選択します。
# MAGIC
# MAGIC 今、以下のセルを実行し、以前に定義された変数が定義されていないことに注意してください。以前のセルを再実行するまでです。

# COMMAND ----------

print(my_name)

# COMMAND ----------

# DBTITLE 0,--i18n-1e11bea0-7be9-4df7-be4e-b525c625dfee
# MAGIC %md
# MAGIC
# MAGIC # 変更内容の確認
# MAGIC
# MAGIC Databricks Repoを使用してこの資料をワークスペースにインポートした場合、このページの左上隅にある**`published`**ブランチボタンをクリックして、3つの変更が表示されるはずです。
# MAGIC 1. 以前のノートブック名で**削除**
# MAGIC 1. 新しいノートブック名で**追加**
# MAGIC 1. 上記にマークダウンセルを作成するための**変更**
# MAGIC
# MAGIC ダイアログを使用して変更を元に戻し、このノートブックを元の状態に復元してください。

# COMMAND ----------

# DBTITLE 0,--i18n-9947d429-2c10-4047-811f-3f5128527c6d
# MAGIC %md
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このラボを完了することで、ノートブックの操作、新しいセルの作成、ノートブック内でのノートブックの実行について自信を持つようになるはずです。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
