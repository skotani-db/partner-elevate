# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 効率的な構造化ストリーミングジョブのスケジューリング
# MAGIC
# MAGIC このノートブックは、共有リソース上で複数のストリームを起動するためのフレームワークとして使用します。
# MAGIC
# MAGIC このノートブックには、すべての更新と追加が部分的にリファクタリングされたコードが含まれており、パイプラインをスケジュールして新しいデータが到着した際に実行することができます。また、 **`bronze`** テーブルからのパーティション削除の処理ロジックも含まれています。
# MAGIC
# MAGIC さらに、各ストリームをスケジューラプールに割り当てるためのロジックも含まれています。以下のコードを確認し、次のセルの指示に従ってストリーミングジョブをスケジュールしてください。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_bronze.png" width="60%" />

# COMMAND ----------

# MAGIC %md
# MAGIC ## このノートブックのスケジュール
# MAGIC
# MAGIC このノートブックは、ジョブクラスタに対してスケジュールされるように設計されていますが、クラスタの起動時間を避けるためにインタラクティブクラスタを使用することもできます。
# MAGIC
# MAGIC なお、汎用クラスタに対して追加のコードを実行すると、クエリの遅延が発生する場合があります。
# MAGIC
# MAGIC このデモにおすすめのクラスタ構成は次のとおりです：
# MAGIC * 最新のDBR（Databricksランタイムのロングテーマサポート版）
# MAGIC * 単一ノードのクラスタ
# MAGIC * ~32コアを持つ単一のVM

# COMMAND ----------

# MAGIC %md 
# MAGIC ## シャッフルパーティション
# MAGIC
# MAGIC いくつかのワークロードによってシャッフルがトリガーされるため、 **`spark.sql.shuffle.partitions`** を管理する必要があります。
# MAGIC
# MAGIC デフォルトのシャッフルパーティションの数（200）は、多くのストリーミングジョブを制限する可能性があります。
# MAGIC
# MAGIC そのため、一般的には、単純に最大コア数を上限として使用し、それより小さい場合はコア数の倍数を維持するのが良い方法です。
# MAGIC
# MAGIC もちろん、この一般的なアドバイスは、単一のクラスタ上で実行されるストリームの数を増やすと変わってきます。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> この値は、各ストリームに対して新しいチェックポイントを作成しない限り、実行間で変更することはできません。

# COMMAND ----------

print(f"Executor cores: {sc.defaultParallelism}")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ウィジェット
# MAGIC
# MAGIC ジョブでは、 **`widgets`** サブモジュールを使用してノートブックにパラメータを渡します。
# MAGIC
# MAGIC **`widgets`** サブモジュールには、インタラクティブクラスタでノートブックを使用する際に対話的な変数を設定するためのいくつかのメソッドがあります。この機能の詳細については、<a href="https://docs.databricks.com/notebooks/widgets.html#widgets" target="_blank">Databricksのドキュメント</a>を参照してください。
# MAGIC
# MAGIC このノートブックでは、ジョブとしてノートブックを実行する際に特に役立つ2つのメソッドに焦点を当てます。
# MAGIC 1. **`dbutils.widgets.text`** は、パラメータ名とデフォルト値を受け取ります。これを使用して、外部の値をスケジュールされたノートブックに渡すことができます。
# MAGIC 2. **`dbutils.widgets.get`** は、パラメータ名を受け取り、その名前のウィジェットから関連する値を取得します。
# MAGIC
# MAGIC これらのメソッドを組み合わせることで、 **`dbutils.widgets.text`** で外部の値を渡すことができ、 **`dbutils.widgets.get`** でその値を参照することができます。
# MAGIC
# MAGIC **注意**: トリガーバッチモードでこのノートブックを実行するには、スケジュールされたジョブにパラメータとしてキー **`once`** と値 **`True`** を渡してください。

# COMMAND ----------

once = eval(dbutils.widgets.get("once"))
print(f"Once: {once}")

# COMMAND ----------

# MAGIC %md 
# MAGIC # ステートストアにRocksDBを使用する
# MAGIC
# MAGIC RocksDBは、クラスタのネイティブメモリとローカルSSDでステートを効率的に管理し、また各ストリームの変更を自動的に提供されたチェックポイントディレクトリに保存します。すべての構造化ストリーミングジョブには必要ではありませんが、ステート情報が多く管理されるクエリには役立ちます。
# MAGIC
# MAGIC **注意**: クエリの再起動間でステート管理スキームを変更することはできません。このノートブックの正常な実行には、スケジュールされたクエリで使用されているチェックポイントが完全にリセットされている必要があります。

# COMMAND ----------

spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ
# MAGIC 次のセルでは、このノートブック全体で使用される変数やパスを読み込みます。
# MAGIC
# MAGIC このノートブックをスケジュールする前に、[Reset Pipelines]($./1 - Reset Pipelines) ノートブックを実行して、テスト用にデータが新しい状態になっていることを確認してください。

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.4.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## カスタムストリーミングクエリリスナー
# MAGIC
# MAGIC 一部の本番ストリーミングアプリケーションでは、ストリーミングクエリの進行状況をリアルタイムで監視する必要があります。
# MAGIC
# MAGIC 通常、これらの結果はリアルタイムのダッシュボード表示のためにパブサブシステムにストリームバックされます。
# MAGIC
# MAGIC ここでは、出力ログをJSONディレクトリに追記し、後でAuto Loaderで読み込むことができるようにします。

# COMMAND ----------

# MAGIC %run ../../Includes/StreamingQueryListener

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto OptimizeとAuto Compaction
# MAGIC
# MAGIC bronze テーブルと3つのパースされた silver テーブルにあまりにも多くの小さなファイルが含まれないようにするため、自動最適化と自動コンパクションを有効にすることが重要です。これらの設定についての詳細は、<a href="https://docs.databricks.com/delta/optimizations/auto-optimize.html" target="_blank">ドキュメントを参照してください</a>。

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze

# COMMAND ----------

date_lookup_df = spark.table("date_lookup").select("date", "week_part")

# COMMAND ----------

def process_bronze(source, table_name, checkpoint, once=False, processing_time="5 seconds"):
    from pyspark.sql import functions as F
    
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"
    
    data_stream_writer = (spark
            .readStream
            .format("cloudFiles")
            .schema(schema)
            .option("maxFilesPerTrigger", 2)
            .option("cloudFiles.format", "json")
            .load(source)
            .join(F.broadcast(date_lookup_df), [F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date")], "left")
            .writeStream
            .option("checkpointLocation", checkpoint)
            .partitionBy("topic", "week_part")
            .queryName("bronze")
         )
    
    if once == True:
        return data_stream_writer.trigger(availableNow=True).table(table_name)
    else:
        return data_stream_writer.trigger(processingTime=processing_time).table(table_name)
        

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apache Sparkスケジューラプールの効率的な設定
# MAGIC
# MAGIC デフォルトでは、ノートブック内で開始されるすべてのクエリは同じ<a href="https://spark.apache.org/docs/latest/job-scheduling.html#scheduling-within-an-application" target="_blank">フェアスケジューリングプール</a>で実行されます。したがって、ノートブック内のすべてのストリーミングクエリから生成されるトリガーによるジョブは、先入れ先出し（FIFO）の順序で連続して実行されます。これにより、クエリに不必要な遅延が発生し、クラスタリソースが効率的に共有されていません。
# MAGIC
# MAGIC 特に、リソースの消費量の多いストリームは、クラスタの利用可能な計算資源を占有し、小規模なストリームが低レイテンシを実現できなくなる可能性があります。スケジューラプールを設定することで、クラスタを最適化して処理時間を確保することができます。
# MAGIC
# MAGIC すべてのストリーミングクエリを同時に実行し、クラスタを効率的に共有するために、クエリを別々のスケジューラプールで実行するように設定できます。この**ローカルプロパティの設定**は、ストリーミングクエリを開始するノートブックの同じセルに記述します。例えば：
# MAGIC
# MAGIC **スケジューラプール1でストリーミングクエリ1を実行する**
# MAGIC
# MAGIC <strong><code>
# MAGIC spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")<br/>
# MAGIC df.writeStream.queryName("query1").format("parquet").start(path1)
# MAGIC </code></strong>
# MAGIC
# MAGIC **スケジューラプール2でストリーミングクエリ2を実行する**
# MAGIC
# MAGIC <strong><code>
# MAGIC spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")<br/>
# MAGIC df.writeStream.queryName("query2").format("delta").start(path2)
# MAGIC </code></strong>

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze")

bronze_query = process_bronze(DA.paths.producer_30m, "bronze_dev", f"{DA.paths.checkpoints}/bronze", once=once)

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Tablesをパースする
# MAGIC
# MAGIC 次のセルでは、 **`heart_rate_silver`** と **`workouts_silver`** の結果に対するクエリを処理するためのPythonクラスを定義しています。

# COMMAND ----------

class Upsert:
    def __init__(self, query, update_temp="stream_updates"):
        self.query = query
        self.update_temp = update_temp 
        
    def upsertToDelta(self, microBatchDF, batch):
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.query)

# COMMAND ----------

# heart_rate_silver
def heart_rate_silver(source_table="bronze", once=False, processing_time="10 seconds"):
    from pyspark.sql import functions as F
    
    query = """
        MERGE INTO heart_rate_silver a
        USING heart_rate_updates b
        ON a.device_id=b.device_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
        """

    streamingMerge=Upsert(query, "heart_rate_updates")
    
    data_stream_writer = (spark
        .readStream
        .option("ignoreDeletes", True)
        .table(source_table)
        .filter("topic = 'bpm'")
        .select(F.from_json(F.col("value").cast("string"), "device_id LONG, time TIMESTAMP, heartrate DOUBLE").alias("v"))
        .select("v.*", F.when(F.col("v.heartrate") <= 0, "Negative BPM").otherwise("OK").alias("bpm_check"))
        .withWatermark("time", "30 seconds")
        .dropDuplicates(["device_id", "time"])
        .writeStream
        .foreachBatch(streamingMerge.upsertToDelta)
        .outputMode("update")
        .option("checkpointLocation", f"{DA.paths.checkpoints}/heart_rate_silver")
        .queryName("heart_rate_silver")
    )
  
    if once == True:
        return data_stream_writer.trigger(availableNow=True).start()
    else:
        return data_stream_writer.trigger(processingTime=processing_time).start()


# COMMAND ----------

# workouts_silver
def workouts_silver(source_table="bronze", once=False, processing_time="15 seconds"):
    from pyspark.sql import functions as F
    
    query = """
        MERGE INTO workouts_silver a
        USING workout_updates b
        ON a.user_id=b.user_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
        """

    streamingMerge=Upsert(query, "workout_updates")
    
    data_stream_writer = (spark
        .readStream
        .option("ignoreDeletes", True)
        .table(source_table)
        .filter("topic = 'workout'")
        .select(F.from_json(F.col("value").cast("string"), "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT").alias("v"))
        .select("v.*")
        .select("user_id", "workout_id", F.col("timestamp").cast("timestamp").alias("time"), "action", "session_id")
        .withWatermark("time", "30 seconds")
        .dropDuplicates(["user_id", "time"])
        .writeStream
        .foreachBatch(streamingMerge.upsertToDelta)
        .outputMode("update")
        .option("checkpointLocation", f"{DA.paths.checkpoints}/workouts_silver")
        .queryName("workouts_silver")

    )

    if once == True:
        return data_stream_writer.trigger(availableNow=True).start()
    else:
        return data_stream_writer.trigger(processingTime=processing_time).start()


# COMMAND ----------

# users

def batch_rank_upsert(microBatchDF, batchId):
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("alt_id").orderBy(F.col("updated").desc())
    
    (microBatchDF
        .filter(F.col("update_type").isin(["new", "update"]))
        .withColumn("rank", F.rank().over(window)).filter("rank == 1").drop("rank")
        .createOrReplaceTempView("ranked_updates"))
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO users u
        USING ranked_updates r
        ON u.alt_id=r.alt_id
        WHEN MATCHED AND u.updated < r.updated
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
    """)

def users_silver(source_table="bronze", once=False, processing_time="30 seconds"):
    from pyspark.sql import functions as F

    schema = """
        user_id LONG, 
        update_type STRING, 
        timestamp FLOAT, 
        dob STRING, 
        sex STRING, 
        gender STRING, 
        first_name STRING, 
        last_name STRING, 
        address STRUCT<
            street_address: STRING, 
            city: STRING, 
            state: STRING, 
            zip: INT
        >"""

    salt = "BEANS"

    data_stream_writer = (spark
        .readStream
        .option("ignoreDeletes", True)
        .table(source_table)
        .filter("topic = 'user_info'")
        .dropDuplicates()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
        .select(F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("alt_id"),
                F.col('timestamp').cast("timestamp").alias("updated"),
                F.to_date('dob','MM/dd/yyyy').alias('dob'),
                'sex', 'gender','first_name','last_name',
                'address.*', "update_type")
        .writeStream
        .foreachBatch(batch_rank_upsert)
        .outputMode("update")
        .option("checkpointLocation", f"{DA.paths.checkpoints}/users")
        .queryName("users")
    )
    
    if once == True:
        return data_stream_writer.trigger(availableNow=True).start()
    else:
        return data_stream_writer.trigger(processingTime=processing_time).start()


# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_parsed")

# COMMAND ----------

heart_rate_silver_query = heart_rate_silver(source_table="bronze_dev", once=once)

# COMMAND ----------

workouts_silver_query = workouts_silver(source_table="bronze_dev", once=once)

# COMMAND ----------

users_query = users_silver(source_table="bronze_dev", once=once)

# COMMAND ----------

if once:
    # While triggered once, we still want them to run in
    #  parallel but, we want to block here until they are done.
    bronze_query.awaitTermination()
    heart_rate_silver_query.awaitTermination()
    workouts_silver_query.awaitTermination()
    users_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 他のレッスンとは異なり、このデモのすべてのノートブックでこれらのアセットが持続するようにするため、 **`DA.cleanup()`** コマンドは実行しません。
# MAGIC
# MAGIC ただし、このデモを永遠に実行しておくわけにはいかないので、30分後にすべてのストリームを停止します。
# MAGIC これは、Streaming Factoryが開始から終了までの時間とほぼ同じです。

# COMMAND ----------

import time
if not once: 
    time.sleep(30*60)
    print("Time's up!")

# COMMAND ----------

# MAGIC %md 30分が経過したので、すべてのストリームを停止します。

# COMMAND ----------

# If once, streams would have auto-terminated.
# Otherwise, we need to stop them now that our 30 min demo is over
for stream in spark.streams.active:
    print(f"Stopping the stream {stream.name}")
    stream.stop()
    stream.awaitTermination()
        
print("All done.")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
