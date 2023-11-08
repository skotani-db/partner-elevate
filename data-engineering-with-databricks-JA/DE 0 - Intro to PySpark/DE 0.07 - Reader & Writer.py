# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC
# MAGIC # Reader & Writer
# MAGIC ##### 目標
# MAGIC 1. CSV ファイルから読み込む
# MAGIC 2. JSON ファイルから読み込む
# MAGIC 3. DataFrame をファイルに書き込む
# MAGIC 4. DataFrame をテーブルに書き込む
# MAGIC 5. DataFrame を Delta テーブルに書き込む
# MAGIC
# MAGIC ##### メソッド
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameReader</a>: **`csv`**, **`json`**, **`option`**, **`schema`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameWriter</a>: **`mode`**, **`option`**, **`parquet`**, **`format`**, **`saveAsTable`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.StructType.html#pyspark.sql.types.StructType" target="_blank">StructType</a>: **`toDDL`**
# MAGIC
# MAGIC ##### Spark タイプ
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#data-types" target="_blank">Types</a>: **`ArrayType`**, **`DoubleType`**, **`IntegerType`**, **`LongType`**, **`StringType`**, **`StructType`**, **`StructField`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.07

# COMMAND ----------

# DBTITLE 0,--i18n-24a8edc0-6f58-4530-a256-656e2b577e3e
# MAGIC %md
# MAGIC
# MAGIC ## DataFrameReader
# MAGIC 外部ストレージシステムからDataFrameを読み込むために使用されるインターフェース
# MAGIC
# MAGIC **`spark.read.parquet("path/to/files")`**
# MAGIC
# MAGIC DataFrameReaderはSparkSessionの属性**`read`**を介してアクセスできます。このクラスにはさまざまな外部ストレージシステムからDataFrameを読み込むためのメソッドが含まれています。

# COMMAND ----------

# DBTITLE 0,--i18n-108685bb-e26b-47db-a974-7e8de357085f
# MAGIC %md
# MAGIC
# MAGIC ### CSVファイルから読み込む
# MAGIC DataFrameReaderの **`csv`** メソッドを使用して、次のオプションでCSVから読み込みます：
# MAGIC
# MAGIC タブ区切り、最初の行をヘッダーとして使用、スキーマを推論します

# COMMAND ----------

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .option("inferSchema", True)
           .csv(DA.paths.users_csv)
          )

users_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-86642c4a-e773-4856-b03a-13b359fa499f
# MAGIC %md
# MAGIC SparkのPython APIでは、DataFrameReaderのオプションを **`csv`** メソッドのパラメータとして指定することもできます。

# COMMAND ----------

users_df = (spark
           .read
           .csv(DA.paths.users_csv, sep="\t", header=True, inferSchema=True)
          )

users_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-8827b582-0b26-407c-ba78-cb64666d7a6b
# MAGIC %md
# MAGIC
# MAGIC カラム名とデータ型を指定して、スキーマを手動で定義します。これは **`StructType`** を作成して行います。

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("email", StringType(), True)
])

# COMMAND ----------

# DBTITLE 0,--i18n-d2e7e50d-afa1-4e65-826f-7eefc0a70640
# MAGIC %md
# MAGIC
# MAGIC このユーザー定義のスキーマを使用して、スキーマを推論する代わりにCSVから読み込みます。

# COMMAND ----------

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .schema(user_defined_schema)
           .csv(DA.paths.users_csv)
          )

# COMMAND ----------

# DBTITLE 0,--i18n-0e098586-6d6c-41a6-9196-640766212724
# MAGIC %md
# MAGIC
# MAGIC または、[データ定義言語（DDL）の構文](https://en.wikipedia.org/wiki/Data_definition_language)を使用してスキーマを定義します。

# COMMAND ----------

ddl_schema = "user_id string, user_first_touch_timestamp long, email string"

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .schema(ddl_schema)
           .csv(DA.paths.users_csv)
          )

# COMMAND ----------

# DBTITLE 0,--i18n-bbc2fc78-c2d4-42c5-91c0-652154ce9f89
# MAGIC %md
# MAGIC
# MAGIC ### Read from JSON files
# MAGIC DataFrameReaderの **`json`** メソッドを使用してJSONから読み込み、スキーマを自動的に推論する方法について説明します。

# COMMAND ----------

events_df = (spark
            .read
            .option("inferSchema", True)
            .json(DA.paths.events_json)
           )

events_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-509e0bc1-1ffd-4c22-8188-c3317215d5e0
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC スキーマ名とデータ型を持つ **`StructType`** を作成することでデータを高速に読み込みます。

# COMMAND ----------

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField("device", StringType(), True),
    StructField("ecommerce", StructType([
        StructField("purchaseRevenue", DoubleType(), True),
        StructField("total_item_quantity", LongType(), True),
        StructField("unique_items", LongType(), True)
    ]), True),
    StructField("event_name", StringType(), True),
    StructField("event_previous_timestamp", LongType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("geo", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("items", ArrayType(
        StructType([
            StructField("coupon", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("item_revenue_in_usd", DoubleType(), True),
            StructField("price_in_usd", DoubleType(), True),
            StructField("quantity", LongType(), True)
        ])
    ), True),
    StructField("traffic_source", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("user_id", StringType(), True)
])

events_df = (spark
            .read
            .schema(user_defined_schema)
            .json(DA.paths.events_json)
           )

# COMMAND ----------

# DBTITLE 0,--i18n-ae248126-23f3-49e2-ab43-63700049405c
# MAGIC %md
# MAGIC
# MAGIC **`StructType`** の Scala メソッド **`toDDL`** を使用すると、DDL 形式の文字列を作成できます。
# MAGIC
# MAGIC これは、CSV や JSON のデータを取り込むために DDL 形式の文字列が必要な場合に便利であり、スキーマの **`StructType`** バリアントを手作りする必要がない場合に役立ちます。
# MAGIC
# MAGIC ただし、この機能は Python では使用できませんが、ノートブックの強力さにより、両方の言語を使用できます。

# COMMAND ----------

# Step 1 - use this trick to transfer a value (the dataset path) between Python and Scala using the shared spark-config
spark.conf.set("com.whatever.your_scope.events_path", DA.paths.events_json)

# COMMAND ----------

# DBTITLE 0,--i18n-5a35a507-1eff-4f5f-b6b9-6a254b61b38f
# MAGIC %md
# MAGIC このようなPythonノートブックで、データを取り込んでDDL形式のスキーマを生成するためにScalaセルを作成できます。

# COMMAND ----------

# MAGIC %scala
# MAGIC // Step 2 - pull the value from the config (or copy & paste it)
# MAGIC val eventsJsonPath = spark.conf.get("com.whatever.your_scope.events_path")
# MAGIC
# MAGIC // Step 3 - Read in the JSON, but let it infer the schema
# MAGIC val eventsSchema = spark.read
# MAGIC                         .option("inferSchema", true)
# MAGIC                         .json(eventsJsonPath)
# MAGIC                         .schema.toDDL
# MAGIC
# MAGIC // Step 4 - print the schema, select it, and copy it.
# MAGIC println("="*80)
# MAGIC println(eventsSchema)
# MAGIC println("="*80)

# COMMAND ----------

# Step 5 - paste the schema from above and assign it to a variable as seen here
events_schema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

# Step 6 - Read in the JSON data using our new DDL formatted string
events_df = (spark.read
                 .schema(events_schema)
                 .json(DA.paths.events_json))

display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-1a79ce6b-d803-4d25-a1c2-c24a70a0d6bf
# MAGIC %md
# MAGIC これは新しいデータセットのスキーマを生成し、開発を加速するための優れた「トリック」です。
# MAGIC
# MAGIC 完了したら（たとえばステップ＃7の場合）、一時的なコードを削除してください。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> 警告: **本番環境でこのトリックを使用しないでください**</br>
# MAGIC スキーマの推論は非常に遅いことがあります。なぜなら、スキーマを推論するためにソースデータセットの完全な読み取りが強制されるためです。

# COMMAND ----------

# DBTITLE 0,--i18n-f57b5940-857f-4e37-a2e4-030b27b3795a
# MAGIC %md
# MAGIC
# MAGIC ## DataFrameWriter
# MAGIC DataFrameを外部ストレージシステムに書き込むためのインターフェース
# MAGIC
# MAGIC <strong><code>
# MAGIC (df  
# MAGIC &nbsp;  .write                         
# MAGIC &nbsp;  .option("compression", "snappy")  
# MAGIC &nbsp;  .mode("overwrite")      
# MAGIC &nbsp;  .parquet(output_dir)       
# MAGIC )
# MAGIC </code></strong>
# MAGIC
# MAGIC DataFrameWriterは、SparkSession属性**`write`**を介してアクセスできます。このクラスには、さまざまな外部ストレージシステムにDataFrameを書き込むためのメソッドが含まれています。

# COMMAND ----------

# DBTITLE 0,--i18n-8799bf1d-1d80-4412-b093-ad3ba71d73b8
# MAGIC %md
# MAGIC
# MAGIC ### データフレームをファイルに書き込む
# MAGIC
# MAGIC **`users_df`** をDataFrameWriterの **`parquet`** メソッドを使用して、以下の設定でParquet形式に書き込んでください。
# MAGIC
# MAGIC Snappy圧縮、上書きモード

# COMMAND ----------

users_output_dir = DA.paths.working_dir + "/users.parquet"

(users_df
 .write
 .option("compression", "snappy")
 .mode("overwrite")
 .parquet(users_output_dir)
)

# COMMAND ----------

display(
    dbutils.fs.ls(users_output_dir)
)

# COMMAND ----------

# DBTITLE 0,--i18n-a2f733f5-8afc-48f0-aebb-52df3a6e461f
# MAGIC %md
# MAGIC DataFrameReaderと同様に、SparkのPython APIではDataFrameWriterのオプションを**`parquet`**メソッドのパラメータとして指定することもできます。

# COMMAND ----------

(users_df
 .write
 .parquet(users_output_dir, compression="snappy", mode="overwrite")
)

# COMMAND ----------

# DBTITLE 0,--i18n-61a5a982-f46e-4cf6-bce2-dd8dd68d9ed5
# MAGIC %md
# MAGIC
# MAGIC ### データフレームをテーブルに書き込む
# MAGIC
# MAGIC データフレームメソッド **`saveAsTable`** を使用して **`events_df`** をテーブルに書き込みます。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> これにより、データフレームメソッド **`createOrReplaceTempView`** で作成されるローカルビューとは異なり、グローバルテーブルが作成されます。

# COMMAND ----------

events_df.write.mode("overwrite").saveAsTable("events")

# COMMAND ----------

# DBTITLE 0,--i18n-abcfcd19-ba89-4d97-a4dd-2fa3a380a953
# MAGIC %md
# MAGIC
# MAGIC このテーブルは、クラスルームセットアップで作成されたデータベースに保存されました。データベース名は以下に表示されます。

# COMMAND ----------

print(DA.schema_name)

# COMMAND ----------

# DBTITLE 0,--i18n-9a929c69-4b77-4c4f-b7b6-2fca645037aa
# MAGIC %md
# MAGIC ## Delta Lake
# MAGIC
# MAGIC ほとんどのケースでは、データが Databricks ワークスペースから参照される場合を含め、Delta Lake フォーマットを使用するのが最適です。
# MAGIC
# MAGIC [Delta Lake](https://delta.io/) は、データ レイクに信頼性をもたらすために Spark と連携するために設計されたオープンソースのテクノロジーです。
# MAGIC
# MAGIC ![delta](https://files.training.databricks.com/images/aspwd/delta_storage_layer.png)
# MAGIC
# MAGIC #### Delta Lake の主な特徴
# MAGIC - ACID トランザクション
# MAGIC - スケーラブルなメタデータ処理
# MAGIC - 統合されたストリーミングとバッチ処理
# MAGIC - タイムトラベル（データのバージョニング）
# MAGIC - スキーマの強制と進化
# MAGIC - オーディット履歴
# MAGIC - Parquet フォーマット
# MAGIC - Apache Spark API と互換性があります

# COMMAND ----------

# DBTITLE 0,--i18n-ba1e0aa1-bd35-4594-9eb7-a16b65affec1
# MAGIC %md
# MAGIC ### Delta Tableへ結果を書き込む
# MAGIC
# MAGIC デルタテーブルに結果を書き込みます。**`events_df`** をDataFrameWriterの **`save`** メソッドを使用して、以下の設定で書き込みます: デルタフォーマットと上書きモード。

# COMMAND ----------

events_output_path = DA.paths.working_dir + "/delta/events"

(events_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(events_output_path)
)

# COMMAND ----------

# DBTITLE 0,--i18n-331a0d38-4573-4987-9aa6-ebfc9476f85d
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
