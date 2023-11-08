-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-2ad42144-605b-486f-ad65-ca24b47b1924
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC # データのクリーニング
-- MAGIC
-- MAGIC データを検査およびクリーンアップする際、データセットに適用する変換を表現するために、さまざまな列式とクエリを構築する必要があります。
-- MAGIC 列式は既存の列、演算子、および組み込み関数から構築されます。これらは新しい列を作成する変換を表現するために **`SELECT`** ステートメントで使用できます。
-- MAGIC 多くの標準SQLクエリコマンド（例：**`DISTINCT`**、**`WHERE`**、**`GROUP BY`**など）がSpark SQLで利用可能で、変換を表現するのに使用できます。
-- MAGIC このノートブックでは、他のシステムと異なるかもしれないいくつかの概念を見直し、一般的な操作のためのいくつかの便利な関数を示します。
-- MAGIC
-- MAGIC **`NULL`** 値に関する動作に特に注意し、文字列と日時フィールドのフォーマットについても説明します。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このレッスンの最後までに、次のことができるようになります：
-- MAGIC - データセットを要約し、NULLの動作を説明する
-- MAGIC - 重複を取得および削除する
-- MAGIC - データセットを予想されるカウント、欠損値、および重複レコードの検証
-- MAGIC - データをクリーンアップおよび変換するための一般的な変換を適用する

-- COMMAND ----------

-- DBTITLE 0,--i18n-2a604768-1aac-40e2-8396-1e15de60cc96
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## セットアップ実行
-- MAGIC
-- MAGIC 以下のセットアップスクリプトを実行すると、データが作成され、このノートブックの残りの実行に必要な値が宣言されます。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.4

-- COMMAND ----------

-- DBTITLE 0,--i18n-31202e20-c326-4fa0-8892-ab9308b4b6f0
-- MAGIC %md
-- MAGIC
-- MAGIC ## データの概要
-- MAGIC
-- MAGIC **`users_dirty`** テーブルから新規ユーザーレコードを使用します。このテーブルのスキーマは次のとおりです：
-- MAGIC
-- MAGIC | フィールド | 型 | 説明 |
-- MAGIC |---|---|---|
-- MAGIC | user_id | string | ユニークな識別子 |
-- MAGIC | user_first_touch_timestamp | long | ユーザーレコードがエポックからのマイクロ秒単位で作成された時間 |
-- MAGIC | email | string | ユーザーがアクションを完了するために提供した最新のメールアドレス |
-- MAGIC | updated | timestamp | このレコードが最後に更新された時間 |
-- MAGIC
-- MAGIC まずは、データの各フィールドに含まれる値をカウントしてみましょう。

-- COMMAND ----------

SELECT count(*), count(user_id), count(user_first_touch_timestamp), count(email), count(updated)
FROM users_dirty

-- COMMAND ----------

-- DBTITLE 0,--i18n-c414c24e-3b72-474b-810d-c3df32032c26
-- MAGIC %md
-- MAGIC
-- MAGIC ## 欠損データの調査
-- MAGIC
-- MAGIC 上記のカウントを基にすると、すべてのフィールドには少なくともいくつかのNULL値があるようです。
-- MAGIC
-- MAGIC **注意:** NULL値は、**`count()`** を含む一部の数学関数で誤った動作をします。
-- MAGIC
-- MAGIC - **`count(col)`** は特定の列または式を数える際に **`NULL`** 値をスキップします。
-- MAGIC - **`count(*)`** は、すべての行の合計数を数える特別な場合です（**`NULL`** 値の行も含みます）。
-- MAGIC
-- MAGIC 特定のフィールド内のNULL値を数えるには、そのフィールドがNULLであるレコードをフィルタリングすることにより、次のいずれかを使用できます：
-- MAGIC **`count_if(col IS NULL)`** または **`col IS NULL`** をフィルタとして使用した **`count(*)`**。
-- MAGIC
-- MAGIC 以下の2つの文は、欠損したメールを持つレコードを正しく数えるものです。

-- COMMAND ----------

SELECT count_if(email IS NULL) FROM users_dirty;
SELECT count(*) FROM users_dirty WHERE email IS NULL;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import col
-- MAGIC usersDF = spark.read.table("users_dirty")
-- MAGIC
-- MAGIC usersDF.selectExpr("count_if(email IS NULL)")
-- MAGIC usersDF.where(col("email").isNull()).count()

-- COMMAND ----------

-- DBTITLE 0,--i18n-ea1ca35c-6421-472b-b70b-4f36bdab6d79
-- MAGIC %md
-- MAGIC  
-- MAGIC ## 重複行の削除
-- MAGIC **`DISTINCT *`** を使用して、行全体が同じ値を含む真の重複レコードを削除できます。

-- COMMAND ----------

SELECT DISTINCT(*) FROM users_dirty

-- COMMAND ----------

-- MAGIC %python
-- MAGIC usersDF.distinct().display()

-- COMMAND ----------

-- DBTITLE 0,--i18n-5da6599b-756c-4d22-85cd-114ff02fc19d
-- MAGIC %md
-- MAGIC
-- MAGIC ## 特定の列を基に重複行を削除する
-- MAGIC
-- MAGIC 以下のコードは、**`user_id`** と **`user_first_touch_timestamp`** の列値に基づいて重複レコードを削除するために **`GROUP BY`** を使用します。 （これらのフィールドは、特定のユーザーが初めてエンカウントされたときに生成され、したがって一意のタプルを形成します。）
-- MAGIC
-- MAGIC ここでは、集計関数 **`max`** をハックとして使用しています：
-- MAGIC - **`email`** と **`updated`** の列の値をグループ化の結果に保持
-- MAGIC - 複数のレコードが存在する場合に非NULLのメールをキャプチャ

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_users AS 
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import max
-- MAGIC dedupedDF = (usersDF
-- MAGIC     .where(col("user_id").isNotNull())
-- MAGIC     .groupBy("user_id", "user_first_touch_timestamp")
-- MAGIC     .agg(max("email").alias("email"), 
-- MAGIC          max("updated").alias("updated"))
-- MAGIC     )
-- MAGIC
-- MAGIC dedupedDF.count()

-- COMMAND ----------

-- DBTITLE 0,--i18n-5e2c98db-ea2d-44dc-b2ae-680dfd85c74b
-- MAGIC %md
-- MAGIC
-- MAGIC 重複を削除し、特定の **`user_id`** と **`user_first_touch_timestamp`** の値に基づいて残っているレコードの期待されるカウントを確認しましょう。

-- COMMAND ----------

SELECT COUNT(DISTINCT(user_id, user_first_touch_timestamp))
FROM users_dirty
WHERE user_id IS NOT NULL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (usersDF
-- MAGIC     .dropDuplicates(["user_id", "user_first_touch_timestamp"])
-- MAGIC     .filter(col("user_id").isNotNull())
-- MAGIC     .count())

-- COMMAND ----------

-- DBTITLE 0,--i18n-776b4ee7-9f29-4a19-89da-1872a1f8cafa
-- MAGIC %md
-- MAGIC
-- MAGIC ## データセットの検証
-- MAGIC 上記の手動のレビューに基づいて、カウントが期待どおりであることを視覚的に確認しました。
-- MAGIC また、シンプルなフィルタと **`WHERE`** 句を使用してプログラム的に検証も行うことができます。
-- MAGIC 各行の **`user_id`** が一意であることを検証してください。

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .groupBy("user_id")
-- MAGIC     .agg(count("*").alias("row_count"))
-- MAGIC     .select((max("row_count") <= 1).alias("no_duplicate_ids")))

-- COMMAND ----------

-- DBTITLE 0,--i18n-d405e7cd-9add-44e3-976a-e56b8cdf9d83
-- MAGIC %md
-- MAGIC
-- MAGIC 各メールが最大で1つの **`user_id`** と関連付けられていることを確認してください。

-- COMMAND ----------

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .where(col("email").isNotNull())
-- MAGIC     .groupby("email")
-- MAGIC     .agg(count("user_id").alias("user_id_count"))
-- MAGIC     .select((max("user_id_count") <= 1).alias("at_most_one_id")))

-- COMMAND ----------

-- DBTITLE 0,--i18n-8630c04d-0752-404f-bfd1-bb96f7b06ffa
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## 日付形式と正規表現
-- MAGIC NULLフィールドを削除し、重複を取り除いた後、データからさらなる価値を抽出したい場合があります。
-- MAGIC
-- MAGIC 以下のコードは、次のことを行います：
-- MAGIC - **`user_first_touch_timestamp`** を有効なタイムスタンプに正しくスケーリングおよびキャスト
-- MAGIC - このタイムスタンプのカレンダー日付と時刻を人間が読める形式で抽出
-- MAGIC - **`正規表現`** を使用して、メールカラムからドメインを抽出
-- MAGIC

-- COMMAND ----------

SELECT *, 
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import date_format, regexp_extract
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .withColumn("first_touch", (col("user_first_touch_timestamp") / 1e6).cast("timestamp"))
-- MAGIC     .withColumn("first_touch_date", date_format("first_touch", "MMM d, yyyy"))
-- MAGIC     .withColumn("first_touch_time", date_format("first_touch", "HH:mm:ss"))
-- MAGIC     .withColumn("email_domain", regexp_extract("email", "(?<=@).+", 0))
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-c9e02918-f105-4c12-b553-3897fa7387cc
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC 以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
