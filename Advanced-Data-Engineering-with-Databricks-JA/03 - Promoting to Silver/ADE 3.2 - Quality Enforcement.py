# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 品質の保証
# MAGIC
# MAGIC Delta Lake を使用してデータを保存する主な動機の一つは、データの品質を保証できることです。スキーマエンフォースメントは自動で行われますが、追加の品質チェックを行うことで、期待に沿ったデータのみがレイクハウスに格納されるようにすることができます。
# MAGIC
# MAGIC このノートブックでは、品質チェックのいくつかのアプローチについて説明します。これらのいくつかは Databricks 固有の機能であり、他は一般的な設計原則です。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、以下のことができるようになります：
# MAGIC - デルタテーブルにチェック制約を追加する
# MAGIC - 検疫テーブルの説明と実装
# MAGIC - デルタテーブルにデータ品質タグを追加するロジックを適用する

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-4.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## テーブル制約
# MAGIC
# MAGIC Databricks では、Delta テーブルに<a href="https://docs.databricks.com/delta/delta-constraints.html" target="_blank">テーブル制約</a>を設定することができます。
# MAGIC
# MAGIC テーブル制約はテーブル内のカラムにブーリアンフィルタを適用し、制約を満たさないデータが書き込まれないようにします。

# COMMAND ----------

# MAGIC %md
# MAGIC 既存のテーブルを見ることから始めよう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC これらが存在する場合、テーブル制約は拡張テーブル記述の **`properties`** の下にリストされます。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED heart_rate_silver

# COMMAND ----------

# MAGIC %md
# MAGIC 制約を定義するときは、必ず人間が読める名前を付けてください。(名前は大文字と小文字を区別しないことに注意してください）。

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE heart_rate_silver ADD CONSTRAINT date_within_range CHECK (time > '2017-01-01');

# COMMAND ----------

# MAGIC %md
# MAGIC テーブルの既存のデータはどれもこの制約に違反していませんでした。名前と実際のチェックの両方が **`properties`** フィールドに表示されます。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED heart_rate_silver

# COMMAND ----------

# MAGIC %md
# MAGIC しかし、制約の条件が満たされない場合はどうなるでしょうか？
# MAGIC
# MAGIC 私たちのデバイスの中には、時折マイナスの **`bpm`** 記録を送信するものがあることがわかっています。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM heart_rate_silver
# MAGIC WHERE heartrate <= 0 

# COMMAND ----------

# MAGIC %md
# MAGIC デルタ・レイクは、既存のレコードが違反する制約を適用することを防いでくれる。

# COMMAND ----------

import pyspark
try:
    spark.sql("ALTER TABLE heart_rate_silver ADD CONSTRAINT validbpm CHECK (heartrate > 0);")
    raise Exception("Expected failure")

except pyspark.sql.utils.AnalysisException as e:
    print("Failed as expected...")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC 制約の適用に失敗したことに注目してほしい。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED heart_rate_silver

# COMMAND ----------

# MAGIC %md
# MAGIC どのように対処すればいいのでしょうか？
# MAGIC
# MAGIC 違反レコードを手動で削除してからチェック制約を設定するか、ブロンズ・テーブルからのデータを処理する前にチェック制約を設定することができます。
# MAGIC
# MAGIC しかし、チェック制約を設定し、データのバッチにそれに違反するレコードが含まれる場合、ジョブは失敗し、エラーをスローします。
# MAGIC
# MAGIC もし悪いレコードを特定しながらもストリーミングジョブを実行し続けることが目的であれば、別のソリューションが必要です。
# MAGIC
# MAGIC ひとつのアイデアは、無効なレコードを隔離することです。
# MAGIC
# MAGIC テーブルから制約を削除する必要がある場合、以下のコードが実行されることに注意してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE heart_rate_silver DROP CONSTRAINT IF EXISTS validbpm;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 隔離
# MAGIC
# MAGIC 隔離の考え方は、不良レコードは別の場所に書き込まれるというものです。
# MAGIC
# MAGIC これにより、正常なデータは効率的に処理され、誤りのあるレコードの追加ロジックや手動のレビュ ーはメインパイプラインから離れて定義され実行されます。
# MAGIC
# MAGIC レコードをうまく救い出すことができたと仮定すると、そのレコードは延期されたシルバーテーブルに簡単に埋め戻すことができる。
# MAGIC
# MAGIC ここでは、 **`foreachBatch`** カスタムライターの中で2つの別々のテーブルに書き込みを行うことで検疫を実装します。

# COMMAND ----------

# MAGIC %md
# MAGIC 正しいスキーマでテーブルを作成することから始める。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bpm_quarantine
# MAGIC     (device_id LONG, time TIMESTAMP, heartrate DOUBLE)
# MAGIC USING DELTA
# MAGIC LOCATION '${da.paths.user_db}/bpm_quarantine'

# COMMAND ----------

# MAGIC %md
# MAGIC Structured Streaming オペレーションでは、追加テーブルへの書き込みは **`foreachBatch`** ロジックの中で行うことができます。
# MAGIC
# MAGIC 以下では、適切な場所にフィルタを追加するようにロジックを更新します。
# MAGIC
# MAGIC 簡単のため、隔離テーブルにデータを挿入する際に重複レコードがないかチェックしません。

# COMMAND ----------

sql_query = """
MERGE INTO heart_rate_silver a
USING stream_updates b
ON a.device_id=b.device_id AND a.time=b.time
WHEN NOT MATCHED THEN INSERT *
"""

class Upsert:
    def __init__(self, query, update_temp="stream_updates"):
        self.query = query
        self.update_temp = update_temp 
        
    def upsert_to_delta(self, micro_batch_df, batch):
        micro_batch_df.filter("heartrate" > 0).createOrReplaceTempView(self.update_temp)
        micro_batch_df._jdf.sparkSession().sql(self.query)
        micro_batch_df.filter("heartrate" <= 0).write.format("delta").mode("append").saveAsTable("bpm_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC  **`foreachBatch`** ロジックの中で、DataFrame 操作は各バッチのデータをストリーミングではなく、静的であるかのように扱っていることに注意してください。
# MAGIC
# MAGIC そのため、 **`writeStream`** の代わりに **`write`** 構文を使用します。
# MAGIC
# MAGIC これはまた、正確に一度だけという保証が緩和されることを意味します。上記の例では、2つのACIDトランザクションがある：
# MAGIC 1. 1.SQLクエリが実行され、シルバーテーブルへの重複レコードの書き込みを避けるために挿入のみのマージを実行します。
# MAGIC 2. 心拍数が負のレコードのマイクロバッチを **`bpm_quarantine`** テーブルに書き込みます。
# MAGIC
# MAGIC 最初のトランザクションが完了した後、2番目のトランザクションが完了する前にジョブが失敗した場合、ジョブの再開時にマイクロバッチロジックを再実行します。
# MAGIC
# MAGIC しかし、挿入のみのマージではすでに重複レコードがテーブルに保存されるのを防いでいるため、データの破損にはつながりません。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## フラグ設定 
# MAGIC 複数回の書き込みや複数テーブルの管理を避けるために、ジョブの失敗を避けながら違反を警告するフラグシステムを実装することができます。
# MAGIC
# MAGIC フラグ付けはオーバーヘッドが少なく、手間のかからないソリューションです。
# MAGIC
# MAGIC これらのフラグは、下流のクエリでフィルタを使用することで、不良データを簡単に切り分けることができます。
# MAGIC
# MAGIC  **`case`** / **`when`** ロジックはこれを簡単にします。
# MAGIC
# MAGIC 以下のセルを実行すると、以下のPySparkコードからコンパイルされたSpark SQLが表示されます。

# COMMAND ----------

from pyspark.sql import functions as F

F.when(F.col("heartrate") <= 0, "Negative BPM").otherwise("OK").alias("bpm_check")

# COMMAND ----------

# MAGIC %md
# MAGIC ここでは、このロジックをブロンズ・データのバッチ・リードに追加変換として挿入し、出力をプレビューしてみます。

# COMMAND ----------

json_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

deduped_df = (spark.read
                  .table("bronze")
                  .filter("topic = 'bpm'")
                  .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                  .select("v.*", F.when(F.col("v.heartrate") <= 0, "Negative BPM")
                                  .otherwise("OK")
                                  .alias("bpm_check"))
                  .dropDuplicates(["device_id", "time"]))

display(deduped_df)

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
