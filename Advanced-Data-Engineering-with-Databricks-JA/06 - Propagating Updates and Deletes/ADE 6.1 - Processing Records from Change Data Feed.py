# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Change Data Feedからのレコードの処理
# MAGIC
# MAGIC このノートブックでは、Delta Lake Change Data Feed (CDF)を使用して、変更をレイクハウスに簡単に伝搬する方法をエンドツーエンドでデモします。
# MAGIC
# MAGIC このデモでは、医療記録の患者情報を表す少し変わったデータセットを扱います。様々な段階でのデータの説明は以下の通りです。
# MAGIC
# MAGIC ### ブロンズテーブル
# MAGIC ここでは、すべてのレコードを消費された時に格納する。行は以下を表す：
# MAGIC 1. 初めてデータを提供する新規患者
# MAGIC 1. 既存の患者が、自分の情報が正しいことを確認する。
# MAGIC 1. 既存患者が自分の情報の一部を更新した場合
# MAGIC
# MAGIC 行が表すアクションのタイプは捕捉されない。
# MAGIC
# MAGIC ### シルバーテーブル
# MAGIC これはデータの検証されたビューです。各患者は、このテーブルに 1 回のみ表示されます。変更された行を識別するために、upsert 文が使用されます。
# MAGIC
# MAGIC ### ゴールド・テーブル
# MAGIC この例では、新しい住所を持つ患者をキャプチャする単純なゴールドテーブルを作成します。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、受講者は以下のことができるようになります：
# MAGIC - クラスタまたは特定のテーブルで変更データフィードを有効にする
# MAGIC - 変更がどのように記録されるかを説明する
# MAGIC - Spark SQL または PySpark を使って CDF 出力を読む
# MAGIC - CDF出力を処理するためにELTコードをリファクタリングする

# COMMAND ----------

# MAGIC %md
# MAGIC ### セットアップ
# MAGIC
# MAGIC 以下のコードでは、いくつかのパス、デモ・データベースを定義し、デモの以前の実行を消去しています。
# MAGIC
# MAGIC また、ソース・ディレクトリの生データをランディングするために使用する別のデータ・ファクトリを定義し、新しいレコードが本番環境に到着したかのように処理できるようにしています。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.1

# COMMAND ----------

# MAGIC %md
# MAGIC ノートブックまたはクラスタでSpark conf設定を使用してCDFを有効にすると、そのスコープで新しく作成されたすべてのDeltaテーブルでCDFが使用されます。

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## オートローダーでデータを取り込む
# MAGIC
# MAGIC ここでは、Auto Loaderを使って、到着したデータを取り込むことにする。
# MAGIC
# MAGIC 以下の手順は以下の通りです：
# MAGIC * ターゲットテーブルの宣言
# MAGIC * ストリームの作成と開始
# MAGIC * ソースディレクトリにデータをロードする

# COMMAND ----------

# MAGIC %md ブロンズテーブルを作成する。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze
# MAGIC   (mrn BIGINT, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, zip BIGINT, city STRING, state STRING, updated timestamp) 
# MAGIC LOCATION '${da.paths.working_dir}/bronze'

# COMMAND ----------

# MAGIC %md ストリームを作成し、開始する。
# MAGIC
# MAGIC この例では
# MAGIC * trigger-onceやtrigger-available-nowとは対照的に、連続処理を使用する。
# MAGIC * スキーマを推論するのではなく、スキーマを指定する。

# COMMAND ----------

schema = "mrn BIGINT, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, zip BIGINT, city STRING, state STRING, updated TIMESTAMP"

bronze_query = (spark.readStream
                     .format("cloudFiles")
                     .option("cloudFiles.format", "json")
                     .schema(schema)
                     .load(DA.paths.cdc_stream)
                     .writeStream
                     .format("delta")
                     .outputMode("append")
                     #.trigger(availableNow=True)
                     .trigger(processingTime='5 seconds')
                     .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
                     .table("bronze"))

DA.block_until_stream_is_ready(bronze_query)

# COMMAND ----------

# MAGIC %md
# MAGIC 上のストリームモニターを展開して、ストリームの進行状況を確認してください。
# MAGIC
# MAGIC ファイルが取り込まれていないはずです。
# MAGIC
# MAGIC 下のセルを使ってデータのバッチをランドします。

# COMMAND ----------

DA.cdc_stream.load()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ターゲットテーブルの作成
# MAGIC
# MAGIC ここでは、 **`DEEP CLONE`** を使用して、読み取り専用データを PROD から DEV 環境（完全な書き込み/削除アクセスが可能）に移動します。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver
# MAGIC DEEP CLONE delta.`${da.paths.silver_source}`
# MAGIC LOCATION '${da.paths.user_db}/silver'

# COMMAND ----------

# MAGIC %md
# MAGIC CDFを有効にして作成されていないテーブルは、デフォルトではCDFが有効になっていないが、以下の構文で変更を取り込むことができる。
# MAGIC
# MAGIC プロパティを編集すると、テーブルがバージョン管理されることに注意してください。
# MAGIC
# MAGIC 注：CDCデータは **`CLONE`** 操作中は**キャプチャされません**。

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED silver

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake によるデータのアップサート
# MAGIC
# MAGIC ここでは、ブロンズテーブルに対するストリーミング読み込みを使用して、シルバーテーブルへのアップサートロジックを定義します、一意の識別子 **`mrn`** にマッチします。
# MAGIC
# MAGIC 新しいレコードを挿入する前に、データのフィールドが変更されたことを確認するために、追加の条件チェックを指定します。

# COMMAND ----------

def upsert_to_delta(microBatchDF, batchId):
    microBatchDF.createOrReplaceTempView("updates")
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO silver s
        USING updates u
        ON s.mrn = u.mrn
        WHEN MATCHED AND s.dob <> u.dob OR
                         s.sex <> u.sex OR
                         s.gender <> u.gender OR
                         s.first_name <> u.first_name OR
                         s.last_name <> u.last_name OR
                         s.street_address <> u.street_address OR
                         s.zip <> u.zip OR
                         s.city <> u.city OR
                         s.state <> u.state OR
                         s.updated <> u.updated
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

# COMMAND ----------

query = (spark.readStream
              .table("bronze")
              .writeStream
              .foreachBatch(upsert_to_delta)
              .outputMode("update")
              # .trigger(availableNow=True)
              .trigger(processingTime='5 seconds')
              .start())

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC テーブル・ディレクトリ **`_change_data`** にネストされたメタデータ・ディレクトリがあることに注意してください。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/silver")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC このディレクトリにはパーケットファイルも含まれていることがわかる。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/silver/_change_data")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 変更データフィードを読む
# MAGIC
# MAGIC 記録されたCDCデータをピックアップするために、2つのオプションを追加する：
# MAGIC - **`readChangeData`**
# MAGIC - **`startingVersion`** (代わりに **`startingTimestamp`** を使用できます)
# MAGIC
# MAGIC ここでは、LA にいる患者だけをストリーミング表示します。変更のあるユーザには 2 つのレコードが存在することに注意してください。

# COMMAND ----------

cdc_df = (spark.readStream
               .format("delta")
               .option("readChangeData", True) # change dataが読み込まれる
               .option("startingVersion", 1) # 変更データ全て、バージョン1から
               .table("silver"))

cdc_la_df = cdc_df.filter("city = 'Los Angeles'")

display(cdc_la_df, streamName = "display_la")
DA.block_until_stream_is_ready(name = "display_la")

# COMMAND ----------

# MAGIC %md
# MAGIC ソース・ディレクトリに別のファイルを置いて数秒待つと、複数の **`_commit_version`** のCDCの変更をキャプチャしていることがわかります（これを見るには、上の表示の **`_commit_version`** カラムのソート順を変更してください）。

# COMMAND ----------

display(cdc_la_df)

# COMMAND ----------

DA.cdc_stream.load()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ゴールドテーブル
# MAGIC ゴールドテーブルでは、新しい住所を持つすべての患者をキャプチャし、この情報を2つのタイムスタンプと一緒に記録します。ソースシステムでこの変更が行われた時刻(現在は **`updated`** と表示されています)と、シルバーテーブルでこの変更が処理された時刻( **`_commit_timestamp`** が生成されたCDCフィールドでキャプチャされます)です。
# MAGIC
# MAGIC シルバーテーブルのCDCレコード内で
# MAGIC - 各レコードの最大 **`_commit_version`** をチェックする。
# MAGIC - 新しいバージョンとアドレスが変更された場合、ゴールドテーブルに挿入する
# MAGIC - レコード **`updated_timestamp`** と **`processed_timestamp`** を挿入します。
# MAGIC
# MAGIC #### Gold Table Schema
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | new_street_address | string |
# MAGIC | new_zip | long |
# MAGIC | new_city | string |
# MAGIC | new_state | string |
# MAGIC | old_street_address | string |
# MAGIC | old_zip | long |
# MAGIC | old_city | string |
# MAGIC | old_state | string |
# MAGIC | updated_timestamp | timestamp |
# MAGIC | processed_timestamp | timestamp |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold (mrn BIGINT,
# MAGIC                    new_street_address STRING,
# MAGIC                    new_zip BIGINT,
# MAGIC                    new_city STRING,
# MAGIC                    new_state STRING,
# MAGIC                    old_street_address STRING,
# MAGIC                    old_zip BIGINT,
# MAGIC                    old_city STRING,
# MAGIC                    old_state STRING,
# MAGIC                    updated_timestamp TIMESTAMP,
# MAGIC                    processed_timestamp TIMESTAMP)
# MAGIC USING DELTA
# MAGIC LOCATION '${da.paths.working_dir}/gold'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 更新が書き込まれたテーブルをストリーミング・ソースとして使用していることに注意してください！
# MAGIC
# MAGIC これは**大きな**付加価値であり、これまで正しく処理するためには大規模な回避策が必要であった。

# COMMAND ----------

silver_stream_df = (spark.readStream
                         .format("delta")
                         .option("readChangeData", True)
                         .option("startingVersion", 1) # バージョン1から
                         .table("silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC **`_change_type`** フィールドにより、有効なレコードと無効なレコードを簡単に区別することができます。
# MAGIC
# MAGIC 新しい有効な行には **`update_postimage`** または **`insert`** ラベルが付きます。
# MAGIC 新しい無効な行には **`update_preimage`** または **`delete`** ラベルが付きます。
# MAGIC
# MAGIC (**注意**：削除を伝播させるロジックについては、もう少し後で説明します)
# MAGIC
# MAGIC 以下のセルでは、ストリーミング・ソースに対して2つのクエリを定義し、データに対してストリーム・ストリーム・マージを実行します。

# COMMAND ----------

from pyspark.sql import functions as F

new_df = (silver_stream_df
         .filter(F.col("_change_type").isin(["update_postimage", "insert"]))
         .selectExpr("mrn",
                     "street_address AS new_street_address",
                     "zip AS new_zip",
                     "city AS new_city",
                     "state AS new_state",
                     "updated AS updated_timestamp",
                     "_commit_timestamp AS processed_timestamp"))
                                                                                         
old_df = (silver_stream_df
         .filter(F.col("_change_type").isin(["update_preimage"]))
         .selectExpr("mrn",
                     "street_address AS old_street_address",
                     "zip AS old_zip",
                     "city AS old_city",
                     "state AS old_state",
                     "_commit_timestamp AS processed_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC **`mrn`** の単一のレコードだけがシルバーテーブルに処理されるように、データを適切に重複排除したと仮定すると、 **`mrn`** と 
# MAGIC  **`_commit_timestamp`** （ここでは **`processed_timestamp`** のエイリアス）は一意の複合キーとして機能します。
# MAGIC
# MAGIC この結合により、データの現在の状態と以前の状態を照合し、すべての変更を追跡できるようになります。
# MAGIC
# MAGIC このテーブルは、住所が更新された患者に対する確認メールや自動メール送信のトリガーなど、さらなる下流処理を推進することができます。
# MAGIC
# MAGIC CDCのデータはストリームとして届くので、シルバーレベルでは新しく変更されたデータのみが処理される。したがって、追加モードでゴールド・テーブルに書き込み、データの粒度を維持することができます。

# COMMAND ----------

query = (new_df.withWatermark("processed_timestamp", "3 minutes")
               .join(old_df, ["mrn", "processed_timestamp"], "left")
               .filter("new_street_address <> old_street_address OR old_street_address IS NULL")
               .writeStream
               .outputMode("append")
               #.trigger(availableNow=True)
               .trigger(processingTime="5 seconds")
               .option("checkpointLocation", f"{DA.paths.checkpoints}/gold")
               .table("gold"))

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ゴールド・テーブルの行数に注目してほしい。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold

# COMMAND ----------

# MAGIC %md
# MAGIC 新しいrawファイルをランディングし、数秒待つと、全ての変更がパイプラインを通して伝播したことがわかります。
# MAGIC
# MAGIC (これは、trigger-onceやtrigger-available-now処理の代わりに、 **`processingTime`** を使用していることを前提としている。ゴールドテーブルのストリーミング書き込みまでスクロールして、処理速度の新しいピークを待つと、データが到着したことがわかります)。

# COMMAND ----------

DA.cdc_stream.load()

# COMMAND ----------

# MAGIC %md
# MAGIC ゴールド・テーブルのレコード数が急増しているのがわかるはずだ。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のセルを実行して、すべてのアクティブなストリームを停止してください。

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()
    stream.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 削除の伝播
# MAGIC
# MAGIC ユースケースによっては、更新や挿入と同時に削除を処理する必要があるかもしれないが、最も重要な削除リクエストは、企業がGDPRやCCPAなどのプライバシー規制のコンプライアンスを維持するためのものである。ほとんどの企業は、これらのリクエストの処理にかかる時間に関するSLAを明示しているが、様々な理由から、これらはコアETLとは別のパイプラインで処理されることが多い。
# MAGIC
# MAGIC ここでは、 **`silver`** テーブルから1人のユーザーを削除しています。

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM silver WHERE mrn = 14125426

# COMMAND ----------

# MAGIC %md
# MAGIC 予想通り、このユーザーを **`silver`** テーブルで見つけようとしても、結果は得られない。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver WHERE mrn = 14125426

# COMMAND ----------

# MAGIC %md
# MAGIC この変更は変更データフィードに記録されています。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("silver", 1)
# MAGIC WHERE mrn = 14125426
# MAGIC ORDER BY _commit_version

# COMMAND ----------

# MAGIC %md
# MAGIC この削除アクションの記録があるので、削除を **`gold`** テーブルに伝搬させるロジックを定義できる。

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deletes AS (
# MAGIC   SELECT mrn
# MAGIC   FROM table_changes("silver", 1)
# MAGIC   WHERE _change_type='delete'
# MAGIC )
# MAGIC
# MAGIC MERGE INTO gold g
# MAGIC USING deletes d
# MAGIC ON d.mrn=g.mrn
# MAGIC WHEN MATCHED
# MAGIC   THEN DELETE

# COMMAND ----------

# MAGIC %md
# MAGIC これにより、ユーザーデータの削除が大幅に簡素化され、ETL で使用するキーとロジックを削除要求の伝搬にも使用できるようになります。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold WHERE mrn = 14125426

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
