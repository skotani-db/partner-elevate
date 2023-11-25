# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # ストリーミング・デザイン・パターン
# MAGIC
# MAGIC Lakehouseは、時間の経過とともに無限に増加するデータセットとシームレスに動作するように最初から設計されています。Spark Structured Streamingは、ほぼリアルタイムのデータ処理ソリューションとして位置づけられることが多いですが、Delta Lakeと組み合わせることで、時間経過に伴うデータの変化を追跡するために必要なオーバーヘッドを大幅に簡素化しながら、増分データの簡単なバッチ処理も提供します。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、受講者は以下を習得する：
# MAGIC - Spark Structured Streamingを使用して、単純なインクリメンタルETLを完了する
# MAGIC - 複数のテーブルへの増分書き込みの実行
# MAGIC - キーバリューストアの値を増分更新する
# MAGIC - **`MERGE`** を使用して、変更データキャプチャ (CDC) データをデルタテーブルに処理する
# MAGIC - 2つのインクリメンタルテーブルを結合
# MAGIC - 増分テーブルとバッチテーブルの結合

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のスクリプトを実行して、必要な変数を設定し、このノートブックの過去の実行を消去する。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-2.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## シンプルなインクリメンタルETL
# MAGIC
# MAGIC ほとんどの組織で処理されている最も大量のデータは、軽い変換と検証を適用しながら、データをある場所から別の場所に移動していると表現することができるでしょう。
# MAGIC
# MAGIC ほとんどのソースデータは時間の経過とともに増加し続けるため、このようなデータをインクリメンタルデータ（ストリーミングデータとも呼ばれる）と呼ぶのが適切である。
# MAGIC
# MAGIC Structured StreamingとDelta Lakeは、インクリメンタルETLを簡単にします。
# MAGIC
# MAGIC 以下では、単純なテーブルを作成し、いくつかの値を挿入します。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE bronze 
# MAGIC (id INT, name STRING, value DOUBLE); 
# MAGIC
# MAGIC INSERT INTO bronze
# MAGIC VALUES (1, "Yve", 1.0),
# MAGIC        (2, "Omar", 2.5),
# MAGIC        (3, "Elia", 3.3)

# COMMAND ----------

# MAGIC %md
# MAGIC 次のセルは、Structured Streaming を使用して作成されたばかりのテーブルのインクリメンタル読み取りを定義し、レコードがいつ処理されたかをキャプチャするフィールドを追加し、1 つのバッチとして新しいテーブルに書き出します。

# COMMAND ----------

from pyspark.sql import functions as F

def update_silver():
    query = (spark.readStream
                  .table("bronze")
                  .withColumn("processed_time", F.current_timestamp())
                  .writeStream.option("checkpointLocation", f"{DA.paths.checkpoints}/silver")
                  .trigger(availableNow=True)
                  .table("silver"))

# COMMAND ----------

update_silver()

# COMMAND ----------

from pyspark.sql import functions as F

def update_silver():
    query = (spark.readStream
                  .table("bronze")
                  .withColumn("processed_time", F.current_timestamp())
                  .writeStream.option("checkpointLocation", f"{DA.paths.checkpoints}/silver")
                  .trigger(availableNow=True)
                  .table("silver"))
    
    query.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC このコードではStructured Streamingを使っているが、これはインクリメンタルな変更を処理するトリガーバッチと考えるのが適切である。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"></img>
# MAGIC 構造化ストリームのデモを容易にするために、 **`trigger(availableNow=True)`** を使ってデータの処理を遅くし、 **`query.awaitTermination()`** と組み合わせて、1つのバッチが処理されるまでレッスンが進まないようにしています。 **`trigger-available-now`** は **`trigger-once`** に非常に似ていますが、1回の大きなバッチではなく、<a href="https://spark.apache.org/releases/spark-release-3-3-0.html" target="_blank">Spark 3.3.0</a> と
# MAGIC <a href="https://docs.databricks.com/release-notes/runtime/10.4.html" target="_blank">Databricks Runtime 10.4 LTS</a>では利用可能なデータがすべて消費されるまで複数のバッチを実行することができます。

# COMMAND ----------

update_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC 予想通り、ストリームはごく短時間実行され、書き込まれた **`silver`** テーブルには、以前に **`bronze`** に書き込まれたすべての値が含まれる。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver
# MAGIC ORDER BY processed_time DESC, id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 新しいレコードの処理は、ソーステーブル **`bronze`** に追加するのと同じくらい簡単です。

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (4, "Ted", 4.7),
# MAGIC        (5, "Tiffany", 5.5),
# MAGIC        (6, "Vini", 6.3)

# COMMAND ----------

# MAGIC %md
# MAGIC ... で、インクリメンタルバッチ処理コードを再実行する。

# COMMAND ----------

update_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC デルタ・レイクは、挿入されたデータを簡単に追跡し、一連のテーブルを通じて伝播させるのに理想的である。このパターンには、"メダリオン"、"マルチホップ"、"デルタ"、"ブロンズ/シルバー/ゴールド "アーキテクチャなど、多くの名前があります。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver
# MAGIC ORDER BY processed_time DESC, id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 複数テーブルへの書き込み
# MAGIC
# MAGIC Structured Streaming に慣れている人は、 **`foreachBatch`** メソッドが、ストリーミングデータの各マイクロバッチに対してカスタムデータ書き込みロジックを実行するオプションを提供していることを知っているかもしれない。
# MAGIC
# MAGIC Databricks ランタイムは、"txnVersion" と "txnAppId" オプションを設定することで、複数のテーブルに書き込む場合でも、これらのストリーミング Delta Lake への書き込みが<a href="https://docs.databricks.com/delta/delta-streaming.html#idempot-write" target="_blank">冪等であることを保証します</a>。これは、複数のテーブルのデータが 1 つのレコードに含まれる場合に特に便利です。これは Databricks Runtime 8.4 で追加されました。
# MAGIC
# MAGIC 以下のコードでは、まず2つの新しいテーブルにレコードを追加するカスタムライターロジックを定義し、次に **`foreachBatch`** 内でこの関数を使用することを示します。
# MAGIC
# MAGIC foreachBatchを使って複数のテーブルに書き込むべきか、単に複数のストリームを使うべきかについては、議論がある。 一般的には、マルチストリームの方がシンプルで効率的な設計です。なぜなら、各テーブルに書き込むストリーミング・ジョブが互いに独立して実行できるからです。 一方、foreachBatchを使用して複数のテーブルに書き込むと、2つのテーブルへの書き込みを同期させることができるという利点があります。

# COMMAND ----------

def write_twice(microBatchDF, batchId):
    appId = "write_twice"
    
    microBatchDF.select("id", "name", F.current_timestamp().alias("processed_time")).write.option("txnVersion", batchId).option("txnAppId", appId).mode("append").saveAsTable("silver_name")
    
    microBatchDF.select("id", "value", F.current_timestamp().alias("processed_time")).write.option("txnVersion", batchId).option("txnAppId", appId).mode("append").saveAsTable("silver_value")


def split_stream():
    query = (spark.readStream.table("bronze")
                 .writeStream
                 .foreachBatch(write_twice)
                 .option("checkpointLocation", f"{DA.paths.checkpoints}/split_stream")
                 .trigger(availableNow=True)
                 .start())
    
    query.awaitTermination()
    

# COMMAND ----------

# MAGIC %md
# MAGIC ストリームが再びトリガーされますが、 **`write_twice`** 関数に含まれる2つの書き込みは Spark バッチ構文を使用していることに注意してください。これは、常に **`foreachBatch`** によって呼び出されるライターの場合です。

# COMMAND ----------

split_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のセルは、ロジックが適切に適用され、初期データが2つのテーブルに分割されたことを示している。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_name
# MAGIC ORDER BY processed_time DESC, id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC これらの各テーブルの **`processed_time`** は若干異なることに注意。上で定義したロジックは、各書き込みが実行された時点の現在のタイムスタンプをキャプチャします。これは、両方の書き込みが同じストリーミング・マイクロバッチ・プロセス内で行われますが、完全に独立したトランザクションであることを示しています（そのため、ダウンストリームのロジックは、若干の非同期更新に寛容でなければなりません）。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_value
# MAGIC ORDER BY processed_time DESC, id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC より多くの値を **`bronze`** テーブルに挿入します。

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (7, "Viktor", 7.4),
# MAGIC        (8, "Hiro", 8.2),
# MAGIC        (9, "Shana", 9.9)

# COMMAND ----------

# MAGIC %md
# MAGIC そして、これらの新しいレコードをピックアップし、2つのテーブルに書き込むことができる。

# COMMAND ----------

split_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC 予想通り、2つのテーブルには新しい値だけが挿入され、また数分間隔で挿入される。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_name
# MAGIC ORDER BY processed_time DESC, id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_value
# MAGIC ORDER BY processed_time DESC, id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## キー・バリュー・ストアで集計を更新する
# MAGIC
# MAGIC インクリメンタルアグリゲーションは、ダッシュボードや現在のサマリーデータによるレポートの充実化など、さまざまな目的で有用です。
# MAGIC
# MAGIC 以下のロジックでは、 **`silver`** テーブルに対する集計を定義しています。

# COMMAND ----------

def update_key_value():
    query = (spark.readStream
                  .table("silver")
                  .groupBy("id")
                  .agg(F.sum("value").alias("total_value"), 
                       F.mean("value").alias("avg_value"),
                       F.count("value").alias("record_count"))
                  .writeStream
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/key_value")
                  .outputMode("complete")
                  .trigger(availableNow=True)
                  .table("key_value"))
    
    query.awaitTermination()
    

# COMMAND ----------

# MAGIC %md
# MAGIC **注**： 上記の変換ではデータをシャッフルする必要があるため、シャッフル・パーティション数を最大コア数に設定すると、より効率的なパフォーマンスが得られます。
# MAGIC
# MAGIC デフォルトのシャッフル・パーティション数（200）は、多くのストリーミング・ジョブを麻痺させる可能性があります。
# MAGIC
# MAGIC そのため、単純に最大コア数をハイエンドとして使用し、それより小さい場合はコア数のファクターを維持するのが合理的で良い方法です。
# MAGIC
# MAGIC 当然ながら、この一般的なアドバイスは、1つのクラスタ上で実行されるストリームの数が増えるにつれて変化します。

# COMMAND ----------

print(f"Executor cores: {sc.defaultParallelism}")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

update_key_value()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM key_value
# MAGIC ORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC **`silver`** テーブルにより多くの値を追加することで、より興味深い集計が可能になります。

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver
# MAGIC VALUES (1, "Yve", 1.0, current_timestamp()),
# MAGIC        (2, "Omar", 2.5, current_timestamp()),
# MAGIC        (3, "Elia", 3.3, current_timestamp()),
# MAGIC        (7, "Viktor", 7.4, current_timestamp()),
# MAGIC        (8, "Hiro", 8.2, current_timestamp()),
# MAGIC        (9, "Shana", 9.9, current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC 注意すべき点は、現在実行されているロジックは、書き込みのたびに結果のテーブルを上書きしているということです。次のセクションでは、既存のレコードを更新するために、 **`MERGE`** を **`foreachBatch`** と組み合わせて使用します。このパターンはキー・バリュー・ストアにも適用できる。

# COMMAND ----------

update_key_value()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM key_value
# MAGIC ORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 変更データ取得データの処理
# MAGIC 様々なシステムから出力される変更データキャプチャ (CDC) データは大きく異なりますが、Databricks を使用してこれらのデータをインクリメンタルに処理することは簡単です。
# MAGIC
# MAGIC ここでは **`bronze_status`** テーブルが行レベルのデータではなく、生の CDC 情報を表します。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze_status 
# MAGIC (user_id INT, status STRING, update_type STRING, processed_timestamp TIMESTAMP);
# MAGIC
# MAGIC INSERT INTO bronze_status
# MAGIC VALUES  (1, "new", "insert", current_timestamp()),
# MAGIC         (2, "repeat", "update", current_timestamp()),
# MAGIC         (3, "at risk", "update", current_timestamp()),
# MAGIC         (4, "churned", "update", current_timestamp()),
# MAGIC         (5, null, "delete", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC 以下の **`silver_status`** テーブルは、指定された **`user_id`** の現在の **`status`** を追跡するために作成されました。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver_status (user_id INT, status STRING, updated_timestamp TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC  **`MERGE`** 文は、受け取った更新の種類に応じて CDC の変更を適切に適用する SQL で簡単に記述できます。
# MAGIC
# MAGIC 残りの **`upsert_cdc`** メソッドには、PySpark DataStreamWriter のマイクロバッチに対して SQL コードを実行するために必要なロジックが含まれています。

# COMMAND ----------

def upsert_cdc(microBatchDF, batchID):
    microBatchDF.createTempView("bronze_batch")
    
    query = """
        MERGE INTO silver_status s
        USING bronze_batch b
        ON b.user_id = s.user_id
        WHEN MATCHED AND b.update_type = "update"
          THEN UPDATE SET user_id=b.user_id, status=b.status, updated_timestamp=b.processed_timestamp
        WHEN MATCHED AND b.update_type = "delete"
          THEN DELETE
        WHEN NOT MATCHED AND b.update_type = "update" OR b.update_type = "insert"
          THEN INSERT (user_id, status, updated_timestamp)
          VALUES (b.user_id, b.status, b.processed_timestamp)
    """
    
    microBatchDF._jdf.sparkSession().sql(query)
    
def streaming_merge():
    query = (spark.readStream
                  .table("bronze_status")
                  .writeStream
                  .foreachBatch(upsert_cdc)
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/silver_status")
                  .trigger(availableNow=True)
                  .start())
    
    query.awaitTermination()
    

# COMMAND ----------

# MAGIC %md
# MAGIC いつものように、新しく到着したレコードをインクリメンタルに処理する。

# COMMAND ----------

streaming_merge()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_status
# MAGIC ORDER BY updated_timestamp DESC, user_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 新しいレコードを挿入することで、これらの変更をシルバー・データに適用することができます。

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_status
# MAGIC VALUES  (1, "repeat", "update", current_timestamp()),
# MAGIC         (2, "at risk", "update", current_timestamp()),
# MAGIC         (3, "churned", "update", current_timestamp()),
# MAGIC         (4, null, "delete", current_timestamp()),
# MAGIC         (6, "new", "insert", current_timestamp())

# COMMAND ----------

streaming_merge()

# COMMAND ----------

# MAGIC %md
# MAGIC 現時点では、このロジックは、順序を無視して到着したデータや重複したレコードに対して、特にロバストではないことに注意してください（ただし、これらの事象は処理可能です）。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_status
# MAGIC ORDER BY updated_timestamp DESC, user_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2つのインクリメンタルテーブルの結合
# MAGIC
# MAGIC インクリメンタルジョインを扱う場合、Watermarkやウィンドウに関する多くの複雑な問題があり、すべてのジョインタイプがサポートされているわけではないことに注意してください。

# COMMAND ----------

def stream_stream_join():
    nameDF = spark.readStream.table("silver_name")
    valueDF = spark.readStream.table("silver_value")
    
    return (nameDF.join(valueDF, nameDF.id == valueDF.id, "inner")
                  .select(nameDF.id, 
                          nameDF.name, 
                          valueDF.value, 
                          F.current_timestamp().alias("joined_timestamp"))
                  .writeStream
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/joined")
                  .queryName("joined_streams_query")
                  .table("joined_streams")
           )

# COMMAND ----------

# MAGIC %md
# MAGIC 上記で定義されたロジックは、 **`trigger`** オプションを設定しないことに注意。
# MAGIC
# MAGIC これは、ストリームが連続実行モードで実行され、デフォルトで500msごとにトリガーされることを意味する。

# COMMAND ----------

query = stream_stream_join()

# COMMAND ----------

# MAGIC %md 
# MAGIC これはまた、新しいテーブルにデータが存在する前に、そのテーブルを読むことが可能であることを意味します。
# MAGIC
# MAGIC ストリームは決して停止しないので、トリガが利用可能な今すぐストリームが終了するまで、 **`awaitTermination()`** でブロックすることはできません。
# MAGIC
# MAGIC その代わりに、 **`query.recentProgress`** を活用することで、"ある "データが処理されるまでブロックすることができます。

# COMMAND ----------

def block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")
    
block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ストリーミング・テーブルで **`display()`** を実行することは、インタラクティブな開発中にテーブルの更新をほぼリアルタイムで監視する方法です。

# COMMAND ----------

display(spark.readStream.table("joined_streams"))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> ストリーミング読み込みがノートブックに表示されると、いつでもストリーミング・ジョブが開始される。
# MAGIC
# MAGIC ここでは2つ目のストリームが開始されている。 1つは元のパイプラインの一部としてデータを処理しています。そして今、2つ目のストリーミング・ジョブが実行され、 **`display()`** 関数を最新の結果で更新しています。

# COMMAND ----------

for stream in spark.streams.active:
    print(stream.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ここでは、 **`bronze`** テーブルに新しい値を追加する。

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (10, "Pedro", 10.5),
# MAGIC        (11, "Amelia", 11.5),
# MAGIC        (12, "Diya", 12.3),
# MAGIC        (13, "Li", 13.4),
# MAGIC        (14, "Daiyu", 14.2),
# MAGIC        (15, "Jacques", 15.9)

# COMMAND ----------

# MAGIC %md
# MAGIC ストリーム-ストリーム結合は、 **`split_stream`** 関数の結果のテーブルに対して設定されます。

# COMMAND ----------

split_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC インタラクティブ・ストリームは、クラスタが自動終了して不要なクラウド・コストが発生する可能性があるため、ノートブック・セッションを終了する前に必ず停止する必要があります。 
# MAGIC 実行中のセルで "Cancel" をクリックするか、ノートブックの一番上にある "Stop Execution" をクリックするか、以下のコードを実行することでストリーミング・ジョブを終了できます。

# COMMAND ----------

for stream in spark.streams.active:
    print(f"Stopping {stream.name}")
    stream.stop()
    stream.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## インクリメンタルデータとスタティックデータの結合
# MAGIC
# MAGIC インクリメンタルテーブルは常に更新されますが、スタティックテーブルは通常、変更または上書きされる可能性のあるデータを含んでいると考えることができます。
# MAGIC
# MAGIC Delta Lake のトランザクション保証とキャッシングにより、Databricks は、静的テーブルに結合されたストリーミングデータの各マイクロバッチが、静的テーブルからのデータの最新バージョンを含むことを保証します。

# COMMAND ----------

statusDF = spark.read.table("silver_status")
bronzeDF = spark.readStream.table("bronze")

query = (bronzeDF.alias("bronze")
                 .join(statusDF.alias("status"), bronzeDF.id==statusDF.user_id, "inner")
                 .select("bronze.*", "status.status")
                 .writeStream
                 .option("checkpointLocation", f"{DA.paths.checkpoints}/join_status")
                 .queryName("joined_status_query")
                 .table("joined_status")
)

# COMMAND ----------

# MAGIC %md 繰り返しになるが、先に進む前にデータが取れるまで待ってほしい。

# COMMAND ----------

block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joined_status
# MAGIC ORDER BY id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ストリームが処理された時点で **`joined_status`** に一致する **`id`** を持つレコードのみが、結果のテーブルに表 示されます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_status
# MAGIC ORDER BY updated_timestamp DESC, user_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 新しいレコードを **`silver_status`** テーブルに処理しても、ストリーム静的結合の結果は自動的に更新されません。

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_status
# MAGIC VALUES  (11, "repeat", "update", current_timestamp()),
# MAGIC         (12, "at risk", "update", current_timestamp()),
# MAGIC         (16, "new", "insert", current_timestamp()),
# MAGIC         (17, "repeat", "update", current_timestamp())

# COMMAND ----------

streaming_merge()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joined_status
# MAGIC ORDER BY id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC クエリのストリーミング側に現れる新しいデータのみが、このパターンを使用してレコードを処理するトリガとなります。

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze
# MAGIC VALUES (16, "Marissa", 1.9),
# MAGIC        (17, "Anne", 2.7)

# COMMAND ----------

# MAGIC %md
# MAGIC ストリーム静的結合のインクリメンタルデータはストリームを "トリガー"し、データの各マイクロバッチが静的テーブルの有効なバージョンに存在する現在の値と結合することを保証する。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joined_status
# MAGIC ORDER BY id DESC

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
