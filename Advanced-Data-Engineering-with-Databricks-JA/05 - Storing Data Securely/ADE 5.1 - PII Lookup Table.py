# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 仮名化PIIルックアップテーブルの作成
# MAGIC
# MAGIC このレッスンでは、潜在的にセンシティブなユーザーデータを保存するための仮名キーを作成します。
# MAGIC
# MAGIC 業界によっては、プライバシーを保証するために、より精巧な非識別化が必要になる場合があります。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_user_lookup.png" width="60%" />
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、生徒は以下を習得する：
# MAGIC - ハッシュする前の「ソルティング」の目的を説明する
# MAGIC - 仮名化のために機密データにソルトハッシングを適用する
# MAGIC - オートローダーを使ってインクリメンタル挿入を処理する

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ
# MAGIC 以下のセルを実行して、関連するデータベースとパスをセットアップすることから始める。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## ブロンズ・データのAuto Load
# MAGIC
# MAGIC 以下のセルは、Auto Loader を使って **`registered_users`** テーブルにインクリメンタルにデータを取り込むロジックを定義し、実行します。
# MAGIC
# MAGIC このロジックは現在、バッチがトリガーされるたびに（現在は10秒ごと）1つのファイルを処理するように設定されています。
# MAGIC
# MAGIC このセルを実行すると、新しいファイルが到着するたびにゆっくりと取り込む常時ストリームが開始される。

# COMMAND ----------

query = (spark.readStream
              .schema("device_id LONG, mac_address STRING, registration_timestamp DOUBLE, user_id LONG")
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.maxFilesPerTrigger", 1)
              .load(DA.paths.raw_user_reg)
              .writeStream
              .option("checkpointLocation", f"{DA.paths.checkpoints}/registered_users")
              .trigger(processingTime="10 seconds")
              .table("registered_users"))

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md 
# MAGIC このレッスンに進む前に、次のことが必要だ：
# MAGIC 1. 既存のストリームを停止する
# MAGIC 2. 作成したテーブルを削除する。
# MAGIC 3. チェックポイント・ディレクトリを消去する。

# COMMAND ----------

query.stop()
query.awaitTermination()
spark.sql("DROP TABLE IF EXISTS registered_users")
dbutils.fs.rm(f"{DA.paths.checkpoints}/registered_users", True)

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のセルを使って、上記のクエリーを、新しいファイルを1つのインクリメンタル・トリガー・バッチとして処理する関数にリファクタリングしてください。
# MAGIC
# MAGIC そのためには
# MAGIC * トリガー毎に処理されるファイルの量を制限するオプションを削除する。
# MAGIC * トリガータイプを "availableNow "に変更する。
# MAGIC * バッチが完了するまで、次のセルの実行をブロックするために、クエリの最後に **`.awaitTermination()`** を追加してください。

# COMMAND ----------

# TODO
def ingest_user_reg():
    query = (spark.readStream
              .schema("device_id LONG, mac_address STRING, registration_timestamp DOUBLE, user_id LONG")
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .load(DA.paths.raw_user_reg)
              .writeStream
              .option("checkpointLocation", f"{DA.paths.checkpoints}/registered_users")
              .trigger(availableNow=True)
              .table("registered_users")
              .awaitTermination()
              )

# COMMAND ----------

# MAGIC %md
# MAGIC 以下の関数を使用してください。
# MAGIC
# MAGIC **注***： デフォルトの動作では、新しいクエリは成功し、古いクエリはエラーになります。

# COMMAND ----------

ingest_user_reg()
display(spark.table("registered_users"))

# COMMAND ----------

# MAGIC %md
# MAGIC 別のカスタム・クラスをセットアップ・スクリプトで初期化し、ソース・ディレクトリに新しいバッチのデータを取り込みます。
# MAGIC
# MAGIC 以下のセルを実行し、テーブルの総行数の違いに注目してください。

# COMMAND ----------

DA.user_reg_stream.load()

ingest_user_reg()
display(spark.table("registered_users"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ナチュラル・キーにソルトを追加する
# MAGIC まず、ソルトをプレーンテキストで定義します。ハッシュ関数を適用して仮名化されたキーを生成する前に、このソルトを自然キーの **`user_id`** と組み合わせます。
# MAGIC
# MAGIC ハッシュの前にソルティングを行うことは、ハッシュを逆引きする辞書攻撃をより高価なものにするため、非常に重要である。 Googleでハッシュを検索して、次の`Secrets123`のSHA-256ハッシュを逆ハッシュしてみてください：`FCF730B6D95236ECD3C9FC2D92D7B6B2BB061514961AEC041D6C7A7192F592E4` この[リンク](https://hashtoolkit.com/decrypt-sha256-hash/fcf730b6d95236ecd3c9fc2d92d7b6b2bb061514961aec041d6c7a7192f592e4) で行います.  次に、 [ここで](https://passwordsgenerator.net/sha256-hash-generator/) `Secrets123:BEANS`をハッシュ化し、同様の検索を行う。 ソルト`BEANS`を追加することでセキュリティが向上したことに注目してほしい。
# MAGIC
# MAGIC より安全性を高めるために、<a href="https://docs.databricks.com/security/secrets/secrets.html" target="_blank">Databricks Secret</a>を使用してソルトをシークレットとしてアップロードすることもできます.

# COMMAND ----------

salt = 'BEANS'
spark.conf.set("da.salt", salt)

# COMMAND ----------

# # If using the Databricks secrets store, here's how you'd read it...
# salt = dbutils.secrets.get(scope="DA-ADE3.03", key="salt")
# salt

# COMMAND ----------

# MAGIC %md
# MAGIC 新しいキーがどのように見えるかをプレビューします。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, sha2(concat(user_id,"${da.salt}"), 256) AS alt_id
# MAGIC FROM registered_users

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL UDF の登録
# MAGIC
# MAGIC SQL ユーザー定義関数を作成して、このロジックを現在のデータベースに **`salted_hash`** という名前で登録します。
# MAGIC
# MAGIC これにより、この関数の適切なパーミッションを持つユーザーであれば誰でもこのロジックを呼び出すことができるようになります。
# MAGIC
# MAGIC UDFは1つのパラメータ： **`String`** を受け入れ、 **`STRING`** を返すようにしてください。 設定されたソルト値にアクセスするには、`"${da.salt}"`という式を使用します。
# MAGIC
# MAGIC For more information, see the <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html" target="_blank">CREATE FUNCTION</a> method from the SQL UDFs docs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC CREATE FUNCTION salted_hash(user_id STRING) RETURNS STRING RETURN sha2(concat(user_id,"${da.salt}"), 256);

# COMMAND ----------

# MAGIC %md
# MAGIC If your SQL UDF is defined correctly, the assert statement below should run without error.

# COMMAND ----------

# Check your work
set_a = spark.sql(f"SELECT sha2(concat('USER123', '{salt}'), 256) alt_id").collect()
set_b = spark.sql("SELECT salted_hash('USER123') alt_id").collect()
assert set_a == set_b, "The 'salted_hash' function is returning the wrong result."
print("All tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ハッシュ関数とソルトが既知であれば、元の鍵と擬似IDを結びつけることは 理論的には可能であることに注意。
# MAGIC
# MAGIC ここでは、難読化のレイヤーを追加するためにこの方法を使用しています。実運用では、もっと洗練されたハッシュ方法を使いたいかもしれません。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ターゲット・テーブルの登録
# MAGIC 以下のロジックは **`user_lookup`** テーブルを作成します。
# MAGIC
# MAGIC ここでは **`user_lookup`** テーブルを作成しているだけです。次のノートブックでは、この擬似IDをユーザーPIIへの唯一のリンクとして使用します。
# MAGIC
# MAGIC **`alt_id`** と他の自然キーのリンクへのアクセスを制御することで、PIIをシステム全体の他のユーザーデータにリンクさせないようにすることができます。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS user_lookup (alt_id string, device_id long, mac_address string, user_id long)
# MAGIC USING DELTA 
# MAGIC LOCATION '${da.paths.working_dir}/user_lookup'

# COMMAND ----------

# MAGIC %md
# MAGIC ## インクリメンタルバッチを処理する関数の定義
# MAGIC
# MAGIC 以下のセルには正しいチェックポイント・パスの設定が含まれています。
# MAGIC
# MAGIC 上記で登録したSQL UDFを適用して、 **`alt_id`** を **`registered_users`** テーブルの **`user_id`** に作成する関数を定義します。
# MAGIC
# MAGIC ターゲットとなる **`user_lookup`** テーブルに必要なカラムがすべて含まれていることを確認してください。トリガーインクリメンタルバッチとして実行するようにロジックを設定する。

# COMMAND ----------

# TODO
def load_user_lookup():
    (spark.readStream
        .table("registered_users")
        .selectExpr("salted_hash(user_id) AS alt_id", "device_id", "mac_address", "user_id")
        .writeStream
        .option("checkpointLocation", f"{DA.paths.checkpoints}/user_lookup")
        .trigger(availableNow=True)
        .table("user_lookup")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のメソッドを使い、結果を表示してください。

# COMMAND ----------

load_user_lookup()

display(spark.table("user_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC インクリメンタル処理がパイプライン全体で機能していることを確認するために、以下の別のデータバッチを処理する。

# COMMAND ----------

DA.user_reg_stream.load()

ingest_user_reg()
load_user_lookup()

display(spark.table("user_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のコードは、 **`user_lookup`** テーブルに100人のユーザーを入れるために、残りのレコードをすべて取り込む。

# COMMAND ----------

DA.user_reg_stream.load(continuous=True)

ingest_user_reg()
load_user_lookup()

display(spark.table("user_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC 次のレッスンでは、この同じハッシュ法をPIIデータの処理と保存に応用する。

# COMMAND ----------

# MAGIC %md 
# MAGIC 以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除します。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
