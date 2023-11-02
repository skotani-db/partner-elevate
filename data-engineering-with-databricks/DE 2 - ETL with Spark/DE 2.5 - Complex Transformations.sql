-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-daae326c-e59e-429b-b135-5662566b6c34
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # 複雑な変換
-- MAGIC
-- MAGIC データレイクハウスに格納されたタブルデータをSpark SQLを使用してクエリすることは、簡単で効率的、かつ高速です。
-- MAGIC
-- MAGIC データ構造が規則的でなくなり、単一のクエリで多くのテーブルを使用する必要がある場合、またはデータの形状を劇的に変更する必要がある場合、これはより複雑になります。このノートブックでは、エンジニアが最も複雑な変換を完了するのに役立つSpark SQLのさまざまな関数を紹介します。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このレッスンの最後までに、あなたは以下のことができるようになるべきです：
-- MAGIC - **`.`** と **`:`** 構文を使用してネストされたデータをクエリする
-- MAGIC - JSON文字列を構造体にパースする
-- MAGIC - 配列と構造体を展開およびアンパックする
-- MAGIC - 結合を使用してデータセットを結合する
-- MAGIC - ピボットテーブルを使用してデータを再形成する
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-b01af8a2-da4a-4c8f-896e-790a60dc8d0c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## セットアップの実行
-- MAGIC
-- MAGIC このセットアップスクリプトは、データを作成し、このノートブックの残りの部分を実行するために必要な値を宣言します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.5

-- COMMAND ----------

-- DBTITLE 0,--i18n-a6be8b8a-1c1f-40dd-a71c-8e91ae079b5c
-- MAGIC %md
-- MAGIC
-- MAGIC ## データの概要
-- MAGIC
-- MAGIC **`events_raw`** テーブルは、Kafkaのペイロードを表すデータに対して登録されました。ほとんどの場合、KafkaデータはバイナリエンコードされたJSON値です。
-- MAGIC これらの値を人間が読み取り可能な形式で表示するために、**`key`** と **`value`** を文字列としてキャストしましょう。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_strings AS 
SELECT string(key), string(value) FROM events_raw;

SELECT * FROM events_strings

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC events_stringsDF = (spark
-- MAGIC     .table("events_raw")
-- MAGIC     .select(col("key").cast("string"), 
-- MAGIC             col("value").cast("string"))
-- MAGIC     )
-- MAGIC display(events_stringsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-67712d1a-cae1-41dc-8f7b-cc97e933128e
-- MAGIC %md
-- MAGIC ## 複雑な型の操作

-- COMMAND ----------

-- DBTITLE 0,--i18n-c6a0cd9e-3bdc-463a-879a-5551fa9a8449
-- MAGIC %md
-- MAGIC
-- MAGIC ### ネストされたデータの操作
-- MAGIC
-- MAGIC 以下のコードセルは、変換された文字列をクエリして、nullフィールドのないJSONオブジェクトの例を表示します（次のセクションで必要になります）。
-- MAGIC
-- MAGIC **注意：** Spark SQLには、JSON文字列または構造体型として保存されたネストされたデータと直接対話するための組み込み機能があります。
-- MAGIC - クエリでサブフィールドにアクセスするために **`:`** 構文を使用します
-- MAGIC - クエリで構造体型のサブフィールドにアクセスするために **`.`** 構文を使用します

-- COMMAND ----------

SELECT * FROM events_strings WHERE value:event_name = "finalize" ORDER BY key LIMIT 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(events_stringsDF
-- MAGIC     .where("value:event_name = 'finalize'")
-- MAGIC     .orderBy("key")
-- MAGIC     .limit(1)
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-914b04cd-a1c1-4a91-aea3-ecd87714ea7d
-- MAGIC %md
-- MAGIC ### 複雑な型の操作
-- MAGIC
-- MAGIC 上記のJSON文字列の例を使用して、スキーマを導出し、次にJSON列全体を構造体型にパースします。
-- MAGIC - **`schema_of_json()`** は、例のJSON文字列から導出されたスキーマを返します。
-- MAGIC - **`from_json()`** は、指定されたスキーマを使用してJSON文字列を含む列を構造体型にパースします。
-- MAGIC
-- MAGIC JSON文字列を構造体型に展開したら、すべての構造体フィールドを列に展開およびフラット化しましょう。
-- MAGIC - **`*`** の展開は構造体をフラットにするのに使用でき、**`col_name.*`** は **`col_name`** のサブフィールドを独自の列に取り出します。

-- COMMAND ----------

SELECT schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}') AS schema

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_events AS SELECT json.* FROM (
SELECT from_json(value, 'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>') AS json 
FROM events_strings);

SELECT * FROM parsed_events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import from_json, schema_of_json
-- MAGIC
-- MAGIC json_string = """
-- MAGIC {"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},"event_name":"finalize","event_previous_timestamp":1593879787820475,"event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress","item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10","item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,"price_in_usd":119.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}
-- MAGIC """
-- MAGIC parsed_eventsDF = (events_stringsDF
-- MAGIC     .select(from_json("value", schema_of_json(json_string)).alias("json"))
-- MAGIC     .select("json.*")
-- MAGIC )
-- MAGIC
-- MAGIC display(parsed_eventsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5ca54e9c-dcb7-4177-99ab-77377ce8d899
-- MAGIC %md
-- MAGIC ### Array配列の操作
-- MAGIC
-- MAGIC Spark SQLには、配列データを操作するためのさまざまな関数が用意されており、以下が含まれます：
-- MAGIC - **`explode()`** は配列の要素を複数の行に分割します。これにより、各要素ごとに新しい行が作成されます。
-- MAGIC - **`size()`** は各行の配列内の要素の数をカウントします。
-- MAGIC
-- MAGIC 以下のコードは、**`items`** フィールド（構造体の配列）を複数の行に展開し、3つ以上のアイテムを含む配列を持つイベントを表示します。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW exploded_events AS
SELECT *, explode(items) AS item
FROM parsed_events;

SELECT * FROM exploded_events WHERE size(items) > 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode, size
-- MAGIC
-- MAGIC exploded_eventsDF = (parsed_eventsDF
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC
-- MAGIC display(exploded_eventsDF.where(size("items") > 2))

-- COMMAND ----------

DESCRIBE exploded_events

-- COMMAND ----------

-- DBTITLE 0,--i18n-0810444d-1ce9-4cb7-9ba9-f4596e84d895
-- MAGIC %md
-- MAGIC 以下のコードは、配列の変換を組み合わせて、ユーザーのカートに含まれるアクションとアイテムの一意のコレクションを表示するテーブルを作成します。
-- MAGIC - **`collect_set()`** は、フィールドの一意の値を収集します。これには配列内のフィールドも含まれます。
-- MAGIC - **`flatten()`** は複数の配列を単一の配列に結合します。
-- MAGIC - **`array_distinct()`** は配列から重複した要素を削除します。

-- COMMAND ----------

SELECT user_id,
  collect_set(event_name) AS event_history,
  array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM exploded_events
GROUP BY user_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import array_distinct, collect_set, flatten
-- MAGIC
-- MAGIC display(exploded_eventsDF
-- MAGIC     .groupby("user_id")
-- MAGIC     .agg(collect_set("event_name").alias("event_history"),
-- MAGIC             array_distinct(flatten(collect_set("items.item_id"))).alias("cart_history"))
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-8744b315-393b-4f8b-a8c1-3d6f9efa93b0
-- MAGIC %md
-- MAGIC  
-- MAGIC ## データの結合と再形成

-- COMMAND ----------

-- DBTITLE 0,--i18n-15407508-ba1c-4aef-bd40-1c8eb244ed83
-- MAGIC %md
-- MAGIC  
-- MAGIC ### テーブルの結合
-- MAGIC
-- MAGIC Spark SQLは標準の **`JOIN`** 操作（inner、outer、left、right、anti、cross、semi）をサポートしています。
-- MAGIC ここでは、展開されたイベントデータセットをルックアップテーブルと結合して、標準のプリントされたアイテム名を取得します。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW item_purchases AS

SELECT * 
FROM (SELECT *, explode(items) AS item FROM sales) a
INNER JOIN item_lookup b
ON a.item.item_id = b.item_id;

SELECT * FROM item_purchases

-- COMMAND ----------

-- MAGIC %python
-- MAGIC exploded_salesDF = (spark
-- MAGIC     .table("sales")
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC
-- MAGIC itemsDF = spark.table("item_lookup")
-- MAGIC
-- MAGIC item_purchasesDF = (exploded_salesDF
-- MAGIC     .join(itemsDF, exploded_salesDF.item.item_id == itemsDF.item_id)
-- MAGIC )
-- MAGIC
-- MAGIC display(item_purchasesDF)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ピボットテーブル
-- MAGIC
-- MAGIC 異なる視点からデータを表示するために、指定されたピボット列内の一意の値を、集計関数に基づいて複数の列に回転させることができる **`PIVOT`** を使用できます。
-- MAGIC - **`PIVOT`** 句は、ピボットテーブルの入力である **`FROM`** 句で指定されたテーブル名またはサブクエリに続きます。
-- MAGIC - ピボット列内の一意の値は、提供された集計式を使用してグループ化および集計され、結果のピボットテーブル内の各一意の値に対して別々の列が作成されます。
-- MAGIC
-- MAGIC 以下のコードセルでは、**`PIVOT`** を使用して、**`sales`** データセットから派生した複数のフィールドに含まれるアイテム購入情報をフラットにします。このフラット化されたデータ形式はダッシュボード作成に役立ちます。

-- COMMAND ----------

SELECT *
FROM item_purchases
PIVOT (
  sum(item.quantity) FOR item_id IN (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K')
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC transactionsDF = (item_purchasesDF
-- MAGIC     .groupBy("order_id", 
-- MAGIC         "email",
-- MAGIC         "transaction_timestamp", 
-- MAGIC         "total_item_quantity", 
-- MAGIC         "purchase_revenue_in_usd", 
-- MAGIC         "unique_items",
-- MAGIC         "items",
-- MAGIC         "item",
-- MAGIC         "name",
-- MAGIC         "price")
-- MAGIC     .pivot("item_id")
-- MAGIC     .sum("item.quantity")
-- MAGIC )
-- MAGIC display(transactionsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-b89c0c3e-2352-4a82-973d-7e655276bede
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
