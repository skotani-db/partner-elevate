# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # パーティションの境界での削除
# MAGIC
# MAGIC シルバーテーブルから個人情報（PII）を削除しましたが、このデータがまだ **`ブロンズ`** テーブルに存在しているという問題に対処していません。
# MAGIC
# MAGIC ストリームの組み合わせ性とマルチプレックスブロンズパターンを使用する設計上の選択により、削除情報を伝播させるためにデルタ変更データフィード（CDF）を有効にするには、各パイプラインを再設計してこの出力を活用する必要があります。CDFを使用せずにテーブルのデータを変更すると、下流の組み合わせ性が壊れます。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_bronze.png" width="60%" />
# MAGIC
# MAGIC このノートブックでは、Deltaテーブルからデータのパーティションを削除する方法と、これらの削除を許可するための増分読み取りの設定方法について学びます。
# MAGIC
# MAGIC この機能は、個人情報（PII）を永久に削除するためだけでなく、あるテーブルから特定の年齢よりも古いデータを削除したい企業にも応用できる便利な機能です。同様に、データはより安価なストレージ層にバックアップされ、それから「アクティブ」または「ホット」なDeltaテーブルから安全に削除されることで、クラウドストレージの節約が実現できます。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このノートブックの終わりまでに、学生は以下のことができるようになります：
# MAGIC - パーティションの境界を使用してデータを削除する
# MAGIC - 下流の増分読み取りを設定してこれらの削除を安全に無視する
# MAGIC - **`VACUUM`** を使用して削除されるファイルを確認し、削除を確定する
# MAGIC - アーカイブされたデータを本番テーブルと結合して完全な履歴データセットを再作成する

# COMMAND ----------

# MAGIC %md
# MAGIC 最初にセットアップスクリプトを実行してください。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.3

# COMMAND ----------

# MAGIC %md
# MAGIC Deltaテーブルは2つのフィールドでパーティション化されています。
# MAGIC
# MAGIC トップレベルのパーティションは **`topic`** 列です。
# MAGIC
# MAGIC セルを実行して、 **`bronze`** テーブルを構成する3つのパーティションディレクトリ（およびDeltaログディレクトリ）を確認してください。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/bronze")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 2番目のレベルのパーティションは **`week_part`** 列であり、これは年と週番号から派生しています。現在、このレベルには約20のディレクトリが存在しています。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/bronze/topic=user_info")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 現在のデータセットでは、これらのファイルで総ユーザー数の一部のみを追跡していることに注意してください。

# COMMAND ----------

total = (spark.table("bronze")
              .filter("topic='user_info'")
              .filter("week_part<='2019-48'")
              .count())
         
print(f"Total: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## データのアーカイブ
# MAGIC 会社が歴史的な記録のアーカイブを維持したい場合（ただし、本番テーブルでは最新のレコードのみを維持する場合）、クラウドネイティブな設定を使用してデータを自動的に低コストのストレージ場所に移動することができます。
# MAGIC
# MAGIC 以下のセルでは、このプロセスをシミュレートしています（ここでは移動ではなくコピーを使用しています）。
# MAGIC
# MAGIC ただし、データファイルとパーティションディレクトリのみが移動されるため、結果のテーブルはデフォルトでParquet形式になります。
# MAGIC
# MAGIC **注意**: 最適なパフォーマンスを得るためには、ディレクトリに対して **`OPTIMIZE`** を実行して小さなファイルを圧縮する必要があります。有効なデータファイルと古いデータファイルはDelta Lakeファイル内で並行して格納されているため、Delta Lakeデータを純粋なParquetテーブルに移動する前に、パーティションごとに **`VACUUM`** を実行して有効なファイルのみがコピーされるようにする必要があります。

# COMMAND ----------

archive_path = f"{DA.paths.working_dir}/pii_archive"
source_path = f"{DA.paths.user_db}/bronze/topic=user_info"

files = dbutils.fs.ls(source_path)
[dbutils.fs.cp(f[0], f"{archive_path}/{f[1]}", True) for f in files if f[1][-8:-1] <= '2019-48'];

spark.sql(f"""
CREATE TABLE IF NOT EXISTS user_info_archived
USING parquet
LOCATION '{archive_path}'
""")

spark.sql("MSCK REPAIR TABLE user_info_archived")

display(spark.sql("SELECT COUNT(*) FROM user_info_archived"))

# COMMAND ----------

# MAGIC %md
# MAGIC ファイルをコピーする際にディレクトリ構造が維持されていることに注意してください。

# COMMAND ----------

files = dbutils.fs.ls(archive_path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## パーティション境界での削除
# MAGIC ここでは、2019年の第49週より前に受信したすべての **`user_info`** を削除するモデルを作成します。
# MAGIC
# MAGIC パーティションの境界をきれいに削除しています。指定された **`week_part`** ディレクトリに含まれるすべてのデータがテーブルから削除されます。

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM bronze 
# MAGIC WHERE topic = 'user_info'
# MAGIC AND week_part <= '2019-48'

# COMMAND ----------

# MAGIC %md
# MAGIC 削除が正常に処理されたかを確認するために、履歴を確認することで確認できます。 **`operationMetrics`** 列には、削除されたファイルの数が示されます。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze

# COMMAND ----------

# MAGIC %md
# MAGIC パーティションの境界で削除する場合、新しいデータファイルを書き出す必要はありません。Deltaログに削除されたファイルとして記録するだけで十分です。
# MAGIC
# MAGIC ただし、ファイルの削除は、テーブルを **`VACUUM`** するまで実際には行われません。
# MAGIC
# MAGIC すべての週のパーティションがまだ **`user_info`** ディレクトリに存在し、各週のディレクトリにデータファイルがまだ存在していることに注意してください。

# COMMAND ----------

files = dbutils.fs.ls(f"{source_path}/week_part=2019-48")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 削除の確認とコミット
# MAGIC
# MAGIC デフォルトでは、Deltaエンジンは7日未満の保持期間での **`VACUUM`** 操作を防止します。以下のセルでは、このチェックを上書きします。

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC **`VACUUM`** 文の最後に **`DRY RUN`** キーワードを追加すると、削除されるファイルを永久に削除する前にプレビューすることができます。
# MAGIC
# MAGIC 現時点では、以下のコマンドを実行することで削除したデータを復元することができます：
# MAGIC
# MAGIC <strong><code>
# MAGIC RESTORE bronze<br/>
# MAGIC TO VERSION AS OF {version}
# MAGIC </code></strong>
# MAGIC
# MAGIC ただし、このコマンドは特定のバージョンのデータを復元するため、バージョンを指定する必要があります。

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM bronze RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC 以下の **`VACUUM`** コマンドを実行すると、これらのファイルが永久に削除されます。

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM bronze RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC 安全のため、常に **`retentionDurationCheck`** を再度有効にすることが最善です。本番環境では、可能な限りこのチェックを上書きすることを避けるべきです（まだDeltaテーブルにコミットされておらず、保持期間の閾値よりも前に書き込まれたファイルに対して他の操作が行われている場合、 **`VACUUM`** はデータの破損を引き起こす可能性があります）。

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC 空のディレクトリは最終的に **`VACUUM`** でクリーンアップされますが、データファイルが空になった後すぐに削除されるわけではありません。

# COMMAND ----------

try:
    files = dbutils.fs.ls(source_path)
    print("The files NOT YET deleted.")
except Exception as e:
    print("The files WERE deleted.")

# COMMAND ----------

# MAGIC %md
# MAGIC そのため、削除文で使用したフィルタと同じフィルタを使用して **`bronze`** テーブルをクエリすると、0件のレコードが返されるはずです。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bronze 
# MAGIC WHERE topic='user_info' AND 
# MAGIC       week_part <= '2019-48'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 完全なテーブル履歴の再作成
# MAGIC
# MAGIC Parquetを使用する場合、ディレクトリのパーティションが結果のデータセットの列として使用されるため、バックアップされたデータにはもはやスキーマに 
# MAGIC  **`topic`** フィールドがありません。
# MAGIC
# MAGIC 以下のロジックは、アーカイブおよび本番データセットに対して **`UNION`** を呼び出して、 **`user_info`** トピックの完全な履歴を再作成する際にこれに対処します。

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH full_bronze_user_info AS (
# MAGIC
# MAGIC   SELECT key, value, partition, offset, timestamp, date, week_part 
# MAGIC   FROM bronze 
# MAGIC   WHERE topic='user_info'
# MAGIC   
# MAGIC   UNION SELECT * FROM user_info_archived) 
# MAGIC   
# MAGIC SELECT COUNT(*) FROM full_bronze_user_info

# COMMAND ----------

# MAGIC %md
# MAGIC ## 変更を無視するようにストリーミング読み取りを更新する
# MAGIC
# MAGIC 以下のセルでは、 **`users`** テーブルへのストリーミング更新を実行するために使用されるすべてのコードがまとめられています。
# MAGIC
# MAGIC このコードを現在実行しようとすると、次の例外が発生します。
# MAGIC > Detected deleted data from streaming source
# MAGIC
# MAGIC セルの22行目では、DataStreamReaderに **`.option("ignoreDeletes", True)`** を追加しています。このオプションだけで、パーティションの削除を伴うDeltaテーブルからのストリーミング処理が有効になります。

# COMMAND ----------

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

unpacked_df = (spark.readStream
                    .option("ignoreDeletes", True)     # This is new!
                    .table("bronze")
                    .filter("topic = 'user_info'")
                    .dropDuplicates()
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
                    .select(F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("alt_id"),
                            F.col('timestamp').cast("timestamp").alias("updated"),
                            F.to_date('dob','MM/dd/yyyy').alias('dob'),
                            'sex', 'gender','first_name','last_name','address.*', "update_type"))



def batch_rank_upsert(microBatchDF, batchId):
    from pyspark.sql.window import Window
    
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

# COMMAND ----------

query = (unpacked_df.writeStream
                    .foreachBatch(batch_rank_upsert)
                    .outputMode("update")
                    .option("checkpointLocation", f"{DA.paths.checkpoints}/batch_rank_upsert")
                    .trigger(once=True)
                    .start())    

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC このコードが完了すると、テーブルのバージョンが増加します。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY users

# COMMAND ----------

# MAGIC %md
# MAGIC ただし、このバージョンのDeltaログファイルを調べると、書き込まれたファイルがデータの変更を示しているだけで、新しいレコードは追加されず、変更されていないことがわかります。

# COMMAND ----------

users_log_path = f"{DA.paths.user_db}/users/_delta_log"
files = dbutils.fs.ls(users_log_path)

max_version = max([file.name for file in files if file.name.endswith(".json")])
display(spark.read.json(f"{users_log_path}/{max_version}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 次のステップ
# MAGIC **`workout`** や **`bpm`** のパーティションではデータを変更していないため、これらは同じ **`bronze`** テーブルから読み取られているため、DataStreamReaderのロジックを変更して変更を無視する必要があります。

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除してください。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
