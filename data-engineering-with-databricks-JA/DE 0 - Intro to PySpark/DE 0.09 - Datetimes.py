# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-0eeddecf-4f2c-4599-960f-8fefe777281f
# MAGIC %md
# MAGIC # Datetime Functions
# MAGIC
# MAGIC ##### 目的
# MAGIC 1. タイムスタンプにキャストする
# MAGIC 2. 日時をフォーマットする
# MAGIC 3. タイムスタンプから抽出する
# MAGIC 4. 日付に変換する
# MAGIC 5. 日時を操作する
# MAGIC
# MAGIC ##### メソッド
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`cast`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions" target="_blank">ビルトイン関数</a>: **`date_format`**, **`to_date`**, **`date_add`**, **`year`**, **`month`**, **`dayofweek`**, **`minute`**, **`second`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.09

# COMMAND ----------

# DBTITLE 0,--i18n-6d2e9c6a-0561-4426-ae88-a8ebca06c61b
# MAGIC %md
# MAGIC
# MAGIC BedBricksのイベントデータセットの一部を使用して、日付と時刻の操作を練習しましょう。

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.table("events").select("user_id", col("event_timestamp").alias("timestamp"))
display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-34540d5e-7a9b-496d-b9d7-1cf7de580f23
# MAGIC %md
# MAGIC
# MAGIC ### Built-In Functions: Date Time Functions
# MAGIC 以下は、Sparkで日付と時刻を操作するためのいくつかの組み込み関数です。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | **`add_months`** | startDateのnumMonths後の日付を返します。 |
# MAGIC | **`current_timestamp`** | クエリ評価の開始時点での現在のタイムスタンプをタイムスタンプ列として返します。 |
# MAGIC | **`date_format`** | 日付形式の指定に従って、日付/タイムスタンプ/文字列を文字列の値に変換します。 |
# MAGIC | **`dayofweek`** | 指定した日付/タイムスタンプ/文字列から整数として月の日を抽出します。 |
# MAGIC | **`from_unixtime`** | Unixエポック（1970-01-01 00:00:00 UTC）からの秒数を、現在のシステムタイムゾーンのyyyy-MM-dd HH:mm:ss形式で表されるタイムスタンプを示す文字列に変換します。 |
# MAGIC | **`minute`** | 指定した日付/タイムスタンプ/文字列から整数として分を抽出します。 |
# MAGIC | **`unix_timestamp`** | 指定されたパターンでの時刻文字列をUnixタイムスタンプ（秒単位）に変換します。 |

# COMMAND ----------

# DBTITLE 0,--i18n-fa5f62a7-e690-48c8-afa0-b446d3bc7aa6
# MAGIC %md
# MAGIC
# MAGIC ### Cast to Timestamp
# MAGIC
# MAGIC #### **`cast()`**
# MAGIC Casts column to a different data type, specified using string representation or DataType.

# COMMAND ----------

timestamp_df = df.withColumn("timestamp", (col("timestamp") / 1e6).cast("timestamp"))
display(timestamp_df)

# COMMAND ----------

from pyspark.sql.types import TimestampType

timestamp_df = df.withColumn("timestamp", (col("timestamp") / 1e6).cast(TimestampType()))
display(timestamp_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6c9cb2b0-ef18-48c4-b1ed-3fad453172c1
# MAGIC %md
# MAGIC
# MAGIC ### 日時
# MAGIC
# MAGIC Sparkでの日時の使用にはいくつか一般的なシナリオがあります。
# MAGIC
# MAGIC - CSV/JSONデータソースは、日時のコンテンツを解析およびフォーマットするためにパターン文字列を使用します。
# MAGIC - StringTypeをDateTypeまたはTimestampTypeに変換する関数、例：**`unix_timestamp`**、**`date_format`**、**`from_unixtime`**、**`to_date`**、**`to_timestamp`**などに関連する日時関数。
# MAGIC
# MAGIC #### フォーマットおよび解析用の日時パターン
# MAGIC Sparkは、日時の解析およびフォーマット用の<a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html" target="_blank">パターン文字列</a>を使用します。これらのパターンの一部は以下に示されています。
# MAGIC
# MAGIC | シンボル | 意味             | 表示形式 | 例                  |
# MAGIC | ------ | --------------- | ------------ | ---------------------- |
# MAGIC | G      | 時代             | テキスト     | AD; Anno Domini        |
# MAGIC | y      | 年                | 年           | 2020; 20               |
# MAGIC | D      | 年間の日数    | 数値(3桁)  | 189                    |
# MAGIC | M/L    | 月               | 月           | 7; 07; Jul; July       |
# MAGIC | d      | 月中の日        | 数値(3桁)  | 28                     |
# MAGIC | Q/q    | 四半期         | 数値/テキスト | 3; 03; Q3; 3rd quarter |
# MAGIC | E      | 曜日             | テキスト     | Tue; Tuesday           |
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> Sparkはバージョン3.0で日付とタイムスタンプの処理方法が変わり、これらの値の解析とフォーマットに使用されるパターンも変わりました。これらの変更についての詳細は、<a href="https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html" target="_blank">このDatabricksのブログポスト</a>を参照してください。

# COMMAND ----------

# DBTITLE 0,--i18n-6bc9e089-fc58-4d8f-b118-d5162b747dc6
# MAGIC %md
# MAGIC
# MAGIC #### 日付のフォーマット
# MAGIC
# MAGIC #### **`date_format()`**
# MAGIC 指定された日時パターンでフォーマットされた文字列に日付/タイムスタンプ/文字列を変換します。

# COMMAND ----------

from pyspark.sql.functions import date_format

formatted_df = (timestamp_df
                .withColumn("date string", date_format("timestamp", "MMMM dd, yyyy"))
                .withColumn("time string", date_format("timestamp", "HH:mm:ss.SSSSSS"))
               )
display(formatted_df)

# COMMAND ----------

# DBTITLE 0,--i18n-adc065e9-e241-424e-ad6e-db1e2cb9b1e6
# MAGIC %md
# MAGIC
# MAGIC #### タイムスタンプから日時属性を抽出
# MAGIC
# MAGIC #### **`year`**
# MAGIC 指定された日付/タイムスタンプ/文字列から整数として年を抽出します。
# MAGIC
# MAGIC ##### 類似のメソッド: **`month`**, **`dayofweek`**, **`minute`**, **`second`** など。

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofweek, minute, second

datetime_df = (timestamp_df
               .withColumn("year", year(col("timestamp")))
               .withColumn("month", month(col("timestamp")))
               .withColumn("dayofweek", dayofweek(col("timestamp")))
               .withColumn("minute", minute(col("timestamp")))
               .withColumn("second", second(col("timestamp")))
              )
display(datetime_df)

# COMMAND ----------

# DBTITLE 0,--i18n-f06bd91b-c4f4-4909-98dd-680fbfdf56cd
# MAGIC %md
# MAGIC
# MAGIC #### 日付に変換
# MAGIC
# MAGIC #### **`to_date`**
# MAGIC カラムをDateTypeに変換し、DateTypeへのキャスティングルールを使用します。

# COMMAND ----------

from pyspark.sql.functions import to_date

date_df = timestamp_df.withColumn("date", to_date(col("timestamp")))
display(date_df)

# COMMAND ----------

# DBTITLE 0,--i18n-8367af41-fc35-44ba-8ab1-df721452e6f3
# MAGIC %md
# MAGIC
# MAGIC ### 日付操作
# MAGIC #### **`date_add`**
# MAGIC 指定された日数後の日付を返します。

# COMMAND ----------

from pyspark.sql.functions import date_add

plus_2_df = timestamp_df.withColumn("plus_two_days", date_add(col("timestamp"), 2))
display(plus_2_df)

# COMMAND ----------

# DBTITLE 0,--i18n-3669ec6f-2f26-4607-9f58-656d463308b5
# MAGIC %md
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
