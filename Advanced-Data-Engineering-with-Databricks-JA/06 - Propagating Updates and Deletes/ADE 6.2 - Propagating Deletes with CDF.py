# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 変更データフィードによる削除の伝播
# MAGIC
# MAGIC ユーザーの PII は、いくつかのアプローチによって仮名化、一般化、冗長化されていますが、削除を Lakehouse で効果的かつ効率的に処理する方法についてはまだ触れていません。
# MAGIC
# MAGIC このノートブックでは、Structured Streaming、Delta Lake、Change Data Feedを組み合わせて、削除リクエストをインクリメンタルに処理し、Lakehouseを通じて削除を伝播するデモを行います。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_users.png" width="60%" />
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、生徒は以下のことができるようになります：
# MAGIC - 重要なイベントを記録するために、任意のメッセージをデルタログにコミットする
# MAGIC - デルタレイク DDL を使用して削除を適用する
# MAGIC - 変更データフィードを使用して削除を伝播する
# MAGIC - 削除が完全にコミットされるように、インクリメンタル構文を活用する
# MAGIC - Change Data Feed のデフォルトデータ保持設定の説明

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のセルを実行して、関連するデータベースとパスを設定することから始める。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 忘れられる権利を満たすための条件
# MAGIC
# MAGIC **`user_lookup`** テーブルには、 **`users`** テーブルの主キーとして使用される **`alt_id`** とレイクハウスの他の場所にあるナチュラルキーとの間のリンクが含まれています。
# MAGIC
# MAGIC 業種が異なれば、データ削除やデータ保持の要件も異なります。ここでは以下を想定する：
# MAGIC 1. **`users`** テーブルのすべてのPIIを削除する必要がある。
# MAGIC 1. 仮名化されたキーと自然キーの間のリンクは忘れるべきである。
# MAGIC 1. 生データソースとログからPIIを含む履歴データを削除するポリシーを制定する。
# MAGIC
# MAGIC このノートブックでは、これらの要件のうち最初の2つに焦点を当てる。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 忘れられる権利申請の処理
# MAGIC
# MAGIC 追加や更新と同時に削除を処理することは可能ですが、忘れられる権利のリクエストに関する罰金は、別個の処理を正当化するかもしれません。
# MAGIC
# MAGIC 以下に、ユーザーデータを通して削除リクエストを処理するためのシンプルなテーブルを設定するロジックを示します。リクエストから30日後というシンプルな期限が挿入され、社内の自動監査がこのテーブルを活用してコンプライアンスを確保できるようになっています。

# COMMAND ----------

from pyspark.sql import functions as F

salt = "BEANS"

schema = """
    user_id LONG, 
    update_type STRING, 
    timestamp FLOAT, 
    dob STRING, 
    sex STRING, 
    gender STRING, 
    first_name STRING, 
    last_name STRING, 
    address STRUCT<street_address: STRING, 
                   city: STRING, 
                   state: STRING, 
                   zip: INT>"""

requests_df = (spark.readStream
                    .table("bronze")
                    .filter("topic = 'user_info'")
                    .dropDuplicates(["value"]) # Drop duplicate data, not just duplicate event deliveries.
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                    .select("v.*", F.col('v.timestamp').cast("timestamp").alias("requested"))
                    .filter("update_type = 'delete'")
                    .select(F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("alt_id"),
                            "requested",
                            F.date_add("requested", 30).alias("deadline"), 
                            F.lit("requested").alias("status")))

# COMMAND ----------

# MAGIC %md
# MAGIC この操作の結果をプレビューする。

# COMMAND ----------

display(requests_df, streamName = "requests")
DA.block_until_stream_is_ready(name = "requests")

# COMMAND ----------

# MAGIC %md
# MAGIC ## コミットメッセージの追加
# MAGIC
# MAGIC デルタレイクは、デルタトランザクションログに記録され、テーブル履歴に表示可能な任意のコミットメッセー ジをサポートしています。これは後の監査に役立ちます。
# MAGIC
# MAGIC SQL でこれを設定すると、グローバルコミットメッセージが作成され、以降のノートブックのすべての操作に使用されます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.commitInfo.userMetadata=Deletes committed

# COMMAND ----------

# MAGIC %md
# MAGIC DataFramesでは、 **`userMetadata`** オプションを使って、書き込みオプションの一部としてコミットメッセージを指定することもできます。
# MAGIC
# MAGIC ここでは、自動化されたジョブではなく、ノートブックでこれらのリクエストを手動で処理していることを示します。

# COMMAND ----------

query = (requests_df.writeStream
                    .outputMode("append")
                    .option("checkpointLocation", f"{DA.paths.checkpoints}/delete_requests")
                    .option("userMetadata", "Requests processed interactively")
                    .trigger(availableNow=True)
                    .table("delete_requests"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC これらのメッセージは、右端の列のテーブル履歴にはっきりと表示される。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## 削除リクエストの処理
# MAGIC
# MAGIC **`delete_requests`** テーブルはユーザーの忘却要求を追跡するために使用されます。通常の **`MERGE`** ステートメントの一部として、既存のデータへの挿入や更新と並行して削除要求を処理することが可能であることに注意してください。
# MAGIC
# MAGIC PIIは現在のレイクハウスを通じて複数の場所に存在するため、リクエストを追跡して非同期に処理することで、低レイテンシSLAを持つ本番ジョブのパフォーマンスが向上する可能性があります。ここでモデル化されたアプローチは、削除が要求された時刻と期限も示し、要求の現在の処理ステータスを示すフィールドを提供する。
# MAGIC
# MAGIC 以下の **`delete_requests`** テーブルを見てください。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## 変更データフィードによるインクリメンタル削除の有効化
# MAGIC
# MAGIC Change Data Feedを使用して、1つのソースから多くのテーブルを削除できるようにします。
# MAGIC
# MAGIC **`user_lookup`** テーブルは異なるパイプライン間で識別情報をリンクしているため、これを削除の伝播元とします。
# MAGIC
# MAGIC まず、テーブルのプロパティを変更し、Change Data Feedを有効にします。

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE user_lookup 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC テーブルの履歴を見て、Change Data Feedが有効になっていることを確認する。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY user_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC 変更データ・フィードはテーブルの初期作成後に有効化されたため、現在のテーブル・バージョンからしか変更データを確認できないことに注意してください。
# MAGIC
# MAGIC 以下のセルは、次のセクションで使用するためにこの値をキャプチャします。

# COMMAND ----------

start_version = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
print(start_version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 削除のコミット
# MAGIC 静的データを扱う場合、削除のコミットは簡単です。
# MAGIC
# MAGIC 以下のロジックは、 **`DELETE`** ステートメントの影響を受けるレコードを含むすべてのデータファイルを書き換えることで **`user_lookup`** テーブルを変更します。Delta Lake では、データの削除は既存のデータファイルを削除するのではなく、新しいデータ ファイルを作成することを思い出してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM user_lookup
# MAGIC WHERE alt_id IN (SELECT alt_id FROM delete_requests WHERE status = 'requested')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 削除の伝播
# MAGIC ここで実装されているレイクハウスアーキテクチャでは通常、 **`user_lookup`** テーブルをインクリメンタルデータとの結合で静的テーブルとして使用しますが、変更データフィードはデータ変更のインクリメンタルな記録として別途活用することができます。
# MAGIC
# MAGIC 以下のコードでは、 **`user_lookup`** テーブルにコミットされたすべての変更のインクリメンタルな読み取りを設定します。

# COMMAND ----------

deleteDF = (spark.readStream
                 .format("delta")
                 .option("readChangeFeed", "true")
                 .option("startingVersion", start_version)
                 .table("user_lookup"))

# COMMAND ----------

# MAGIC %md
# MAGIC 自然キー ( **`user_id`** , **`device_id`** , **`mac_address`** ) の関係は **`user_lookup`** に格納されます。これらにより、様々なパイプライン/ソース間でユーザーのデータをリンクすることができます。このテーブルからの変更データフィードはこれらすべてのフィールドを保持し、下流のテーブルで削除または変更されるレコードを正しく識別できるようにします。
# MAGIC
# MAGIC 以下の関数は、異なるキーと構文を使用して 2 つのテーブルに削除をコミットしています。この場合、 **`users`** テーブルへの削除を処理するために、実演している **`MERGE`** 構文は必要ないことに注意してください。このコードブロックは、挿入と更新を削除と同じコードブロックで処理する場合に拡張できる基本構文を実演しています。
# MAGIC
# MAGIC この2つのテーブルの変更が正常に完了すると、 **`delete_requests`** テーブルに更新が処理されます。 **`user_lookup`** テーブルから正常に削除されたデータを利用して、 **`delete_requests`** テーブルの値を更新していることに注意してください。

# COMMAND ----------

def process_deletes(microBatchDF, batchId):
    
    (microBatchDF
        .filter("_change_type = 'delete'")
        .createOrReplaceTempView("deletes"))
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO users u
        USING deletes d
        ON u.alt_id = d.alt_id
        WHEN MATCHED
            THEN DELETE
    """)

    microBatchDF._jdf.sparkSession().sql("""
        DELETE FROM user_bins
        WHERE user_id IN (SELECT user_id FROM deletes)
    """)
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO delete_requests dr
        USING deletes d
        ON d.alt_id = dr.alt_id
        WHEN MATCHED
          THEN UPDATE SET status = "deleted"
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC この作業負荷は、 **`user_lookup`** テーブル（変更データフィードを通して追跡される）への増分変更によって駆動されていることを思い出してください。
# MAGIC
# MAGIC 以下のセルを実行すると、1つのテーブルの削除がレイクハウス全体の複数のテーブルに伝搬されます。

# COMMAND ----------

query = (deleteDF.writeStream
                 .foreachBatch(process_deletes)
                 .outputMode("update")
                 .option("checkpointLocation", f"{DA.paths.checkpoints}/deletes")
                 .trigger(availableNow=True)
                 .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 削除コミットの確認
# MAGIC 現在の実装では、ユーザー登録が **`user_lookup`** テーブルに登録されなかった場合、このユーザーのデータは他のテーブルから削除されないことに注意してください。しかし、 **`delete_requests`** テーブルのこれらのレコードのステータスも **`requested`** のままなので、必要に応じて冗長なアプローチを適用することができます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC コミットメッセージは履歴の右端の列、 **`userMetadata`** の下にあることに注意してください。
# MAGIC
# MAGIC **`users`** テーブルでは、削除のみがコミットされたにもかかわらず、選択された構文のため、ヒストリの操作フィールドはマージを示します。削除された行の数は、 **`operationMetrics`** で確認することができます。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY users

# COMMAND ----------

# MAGIC %md
# MAGIC 予想通り、 **`user_bin`** は削除を表示する。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY user_bins

# COMMAND ----------

# MAGIC %md
# MAGIC **`delete_requests`** への変更もマージ操作を示しており、このテーブルではレコードが削除されたのではなく、更新されたことを適切に示している。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## 削除は完全にコミットされるのか？
# MAGIC
# MAGIC 正確ではありません。
# MAGIC
# MAGIC Delta Lake の履歴と CDF 機能が実装されているため、削除された値は古いバージョンのデータにも残っています。
# MAGIC
# MAGIC 以下のクエリは、 **`user_bin`** テーブルのv1で削除されたレコードを示しています。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM user_bins@v0 u1
# MAGIC EXCEPT 
# MAGIC SELECT * FROM user_bins u2

# COMMAND ----------

# MAGIC %md
# MAGIC 同様に、 **`user_lookup`** テーブルにコミットされた削除によって生成された増分データに対してすでにロジックを適用していますが、この情報は変更フィード内でまだ利用可能です。

# COMMAND ----------

df = (spark.read
           .option("readChangeFeed", "true")
           .option("startingVersion", start_version)
           .table("user_lookup")
           .filter("_change_type = 'delete'"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 次のノートブックでは、これらの削除を完全にコミットすることを検討するとともに、PIIを含む過去の生データへのアクセスを削除するためのガイダンスを提供する。

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
