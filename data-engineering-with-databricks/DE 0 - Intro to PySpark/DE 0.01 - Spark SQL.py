# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-ad7af192-ab00-41a3-b683-5de4856cacb0
# MAGIC %md
# MAGIC # Spark SQL
# MAGIC
# MAGIC Spark SQLの基本的な概念をDataFrame APIを使って示します。
# MAGIC
# MAGIC ##### 目標
# MAGIC 1. SQLクエリを実行する
# MAGIC 1. テーブルからDataFrameを作成する
# MAGIC 1. DataFrame変換を使用して同じクエリを書く
# MAGIC 1. DataFrameアクションで計算をトリガーする
# MAGIC 1. DataFramesとSQLの間で変換する
# MAGIC
# MAGIC ##### 方法
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">SparkSession</a>: **`sql`**, **`table`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>:
# MAGIC   - 変換: **`select`**, **`where`**, **`orderBy`**
# MAGIC   - アクション: **`show`**, **`count`**, **`take`**
# MAGIC   - その他の方法: **`printSchema`**, **`schema`**, **`createOrReplaceTempView`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.01

# COMMAND ----------

# DBTITLE 0,--i18n-3ad6c2cb-bfa4-4af5-b637-ba001a9ef54b
# MAGIC %md
# MAGIC
# MAGIC ## 複数のインターフェース
# MAGIC Spark SQLは、複数のインターフェースを持つ構造化データ処理のモジュールです。
# MAGIC
# MAGIC Spark SQLとは2つの方法でやり取りすることができます：
# MAGIC 1. SQLクエリを実行する
# MAGIC 1. DataFrame APIを利用する

# COMMAND ----------

# DBTITLE 0,--i18n-236a9dcf-8e89-4b08-988a-67c3ca31bb71
# MAGIC %md
# MAGIC **方法1: SQLクエリを実行する**
# MAGIC
# MAGIC これは基本的なSQLクエリです。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, price
# MAGIC FROM products
# MAGIC WHERE price < 200
# MAGIC ORDER BY price

# COMMAND ----------

# DBTITLE 0,--i18n-58f7e711-13f5-4015-8cff-c18ec5b305c6
# MAGIC %md
# MAGIC
# MAGIC **方法2: DataFrame APIを利用する**
# MAGIC
# MAGIC DataFrame APIを使用してSpark SQLクエリを表現することもできます。
# MAGIC 以下のセルは、上記で取得した結果と同じ結果を含むDataFrameを返します。

# COMMAND ----------

display(spark
        .table("products")
        .select("name", "price")
        .where("price < 200")
        .orderBy("price")
       )

# COMMAND ----------

# DBTITLE 0,--i18n-5b338899-be0c-46ae-92d9-8cfc3c2c3fb8
# MAGIC %md
# MAGIC
# MAGIC レッスンの後半でDataFrame APIの構文について詳しく説明しますが、このビルダーデザインパターンにより、SQLと非常に似た一連の操作を連鎖させることができることがわかります。

# COMMAND ----------

# DBTITLE 0,--i18n-bb02bfff-cf98-4639-af21-76bec5c8d95b
# MAGIC %md
# MAGIC
# MAGIC ## クエリ実行
# MAGIC 私たちは任意のインターフェースを使用して同じクエリを表現することができます。Spark SQLエンジンは、私たちのSparkクラスター上で最適化および実行するために使用される同じクエリプランを生成します。
# MAGIC
# MAGIC ![クエリ実行エンジン](https://files.training.databricks.com/images/aspwd/spark_sql_query_execution_engine.png)
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> Resilient Distributed Datasets (RDDs)は、Sparkクラスターによって処理されるデータセットの低レベルの表現です。Sparkの初期のバージョンでは、<a href="https://spark.apache.org/docs/latest/rdd-programming-guide.html" target="_blank">直接RDDを操作するコード</a>を書く必要がありました。現代のSparkのバージョンでは、代わりに高レベルのDataFrame APIを使用するべきであり、Sparkは自動的に低レベルのRDD操作にコンパイルします。

# COMMAND ----------

# DBTITLE 0,--i18n-fbaea5c1-fefc-4b3b-a645-824ffa77bbd5
# MAGIC %md
# MAGIC
# MAGIC ## Spark APIドキュメント
# MAGIC
# MAGIC Spark SQLでDataFramesをどのように扱うかを学ぶには、まずSpark APIドキュメントを見てみましょう。
# MAGIC 主なSparkの<a href="https://spark.apache.org/docs/latest/" target="_blank">ドキュメント</a>ページには、Sparkの各バージョンに対するAPIドキュメントと便利なガイドへのリンクが含まれています。
# MAGIC
# MAGIC <a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/index.html" target="_blank">Scala API</a>および<a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">Python API</a>は最も一般的に使用されており、両言語のドキュメントを参照することがしばしば役立ちます。
# MAGIC Scalaのドキュメントはより包括的であり、Pythonのドキュメントはより多くのコード例を持っています。
# MAGIC
# MAGIC #### Spark SQLモジュールのドキュメントのナビゲーション
# MAGIC Scala APIの**`org.apache.spark.sql`**またはPython APIの**`pyspark.sql`**に移動して、Spark SQLモジュールを見つけます。
# MAGIC このモジュールで最初に探るクラスは**`SparkSession`**クラスです。検索バーに"SparkSession"と入力することでこれを見つけることができます。

# COMMAND ----------

# DBTITLE 0,--i18n-24790eda-96df-49bb-af34-b1ed839fa80a
# MAGIC %md
# MAGIC ## SparkSession
# MAGIC The **`SparkSession`** class is the single entry point to all functionality in Spark using the DataFrame API.
# MAGIC
# MAGIC In Databricks notebooks, the SparkSession is created for you, stored in a variable called **`spark`**.

# COMMAND ----------

spark

# COMMAND ----------

# DBTITLE 0,--i18n-4f5934fb-12b9-4bf2-b821-5ab17d627309
# MAGIC %md
# MAGIC
# MAGIC このレッスンの最初の例では、SparkSessionメソッドの **`table`** を使用して、**`products`** テーブルからDataFrameを作成しました。これを変数 **`products_df`** に保存しましょう。

# COMMAND ----------

products_df = spark.table("products")

# COMMAND ----------

# DBTITLE 0,--i18n-f9968eff-ed08-4ed6-9fe7-0252b94bf50a
# MAGIC %md
# MAGIC 以下は、DataFrameを作成するために使用できるいくつかの追加メソッドです。これらすべては、**`SparkSession`** の<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">ドキュメント</a>で見つけることができます。
# MAGIC
# MAGIC #### **`SparkSession`** メソッド
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | sql | 与えられたクエリの結果を表すDataFrameを返します |
# MAGIC | table | 指定されたテーブルをDataFrameとして返します |
# MAGIC | read | DataFrameとしてデータを読み込むために使用できるDataFrameReaderを返します |
# MAGIC | range | スタートからエンドまで（エンドは除外）の範囲の要素を含む列を持つDataFrameを作成し、ステップ値とパーティションの数を指定します |
# MAGIC | createDataFrame | 主にテスト用に、タプルのリストからDataFrameを作成します |

# COMMAND ----------

# DBTITLE 0,--i18n-2277e250-91f9-489a-940b-97d17e75c7f5
# MAGIC %md
# MAGIC
# MAGIC SparkSessionメソッドを使ってSQLを実行してみましょう。
# MAGIC
# MAGIC

# COMMAND ----------

result_df = spark.sql("""
SELECT name, price
FROM products
WHERE price < 200
ORDER BY price
""")

display(result_df)

# COMMAND ----------

# DBTITLE 0,--i18n-f2851702-3573-4cb4-9433-ec31d4ceb0f2
# MAGIC %md
# MAGIC
# MAGIC ## DataFrames
# MAGIC DataFrame APIのメソッドを使用してクエリを表現すると、結果はDataFrameで返されることを思い出してください。これを変数 **`budget_df`** に保存しましょう。
# MAGIC
# MAGIC **DataFrame** は、名前付きの列にグループ化されたデータの分散コレクションです。
# MAGIC
# MAGIC

# COMMAND ----------

budget_df = (spark
             .table("products")
             .select("name", "price")
             .where("price < 200")
             .orderBy("price")
            )

# COMMAND ----------

# DBTITLE 0,--i18n-d538680a-1d7a-433c-9715-7fd975d4427b
# MAGIC %md
# MAGIC
# MAGIC **`display()`** を使って、dataframeの結果を出力することができます。

# COMMAND ----------

display(budget_df)

# COMMAND ----------

# DBTITLE 0,--i18n-ea532d26-a607-4860-959a-00a2eca34305
# MAGIC %md
# MAGIC
# MAGIC **スキーマ**は、dataframeの列名と型を定義します。
# MAGIC
# MAGIC **`schema`** 属性を使用して、dataframeのスキーマにアクセスします。

# COMMAND ----------

budget_df.schema

# COMMAND ----------

# DBTITLE 0,--i18n-4212166a-a200-44b5-985c-f7f1b33709a3
# MAGIC %md
# MAGIC
# MAGIC View a nicer output for this schema using the **`printSchema()`** method.

# COMMAND ----------

budget_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-7ad577db-093a-40fb-802e-99bbc5a4435b
# MAGIC %md
# MAGIC
# MAGIC ## 変換
# MAGIC **`budget_df`** を作成したとき、DataFrame変換メソッドの一連の例、つまり **`select`**, **`where`**, **`orderBy`** を使用しました。
# MAGIC
# MAGIC <strong><code>products_df  
# MAGIC &nbsp;  .select("name", "price")  
# MAGIC &nbsp;  .where("price < 200")  
# MAGIC &nbsp;  .orderBy("price")  
# MAGIC </code></strong>
# MAGIC     
# MAGIC 変換は、DataFrames上で動作し、DataFramesを返すため、新しいDataFramesを構築するために変換メソッドを連鎖させることができます。
# MAGIC しかし、これらの操作は単独で実行することはできず、変換メソッドは**遅延評価**されます。
# MAGIC
# MAGIC 以下のセルを実行しても、計算はトリガーされません。

# COMMAND ----------

(products_df
  .select("name", "price")
  .where("price < 200")
  .orderBy("price"))

# COMMAND ----------

# DBTITLE 0,--i18n-56f40b55-842f-44cf-b34a-b0fd17a962d4
# MAGIC %md
# MAGIC
# MAGIC ## アクション
# MAGIC 逆に、DataFrameアクションは**計算をトリガーする**メソッドです。
# MAGIC アクションは、DataFrame変換の実行をトリガーするために必要です。
# MAGIC
# MAGIC **`show`** アクションは、以下のセルに変換を実行させます。

# COMMAND ----------

(products_df
  .select("name", "price")
  .where("price < 200")
  .orderBy("price")
  .show())

# COMMAND ----------

# DBTITLE 0,--i18n-6f574091-0026-4dd2-9763-c6d4c3b9c4fe
# MAGIC %md
# MAGIC
# MAGIC 以下は、<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#dataframe-apis" target="_blank">DataFrame</a>アクションのいくつかの例です。
# MAGIC
# MAGIC ### DataFrameアクションメソッド
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | show | DataFrameの上位n行を表形式で表示します |
# MAGIC | count | DataFrameの行数を返します |
# MAGIC | describe, summary | 数値および文字列の列に対する基本統計を計算します |
# MAGIC | first, head | 最初の行を返します |
# MAGIC | collect | このDataFrameのすべての行を含む配列を返します |
# MAGIC | take | DataFrameの最初のn行の配列を返します |

# COMMAND ----------

# DBTITLE 0,--i18n-7e725f41-43bc-4e56-9c44-d46becd375a0
# MAGIC %md
# MAGIC **`count`** はDataFrameのレコード数を返します。
# MAGIC

# COMMAND ----------

budget_df.count()

# COMMAND ----------

# DBTITLE 0,--i18n-12ea69d5-587e-4953-80d9-81955eeb9d7b
# MAGIC %md
# MAGIC **`collect`** はDataFrameのすべての行の配列を返します。

# COMMAND ----------

budget_df.collect()

# COMMAND ----------

# DBTITLE 0,--i18n-983f5da8-b456-42b5-b21c-b6f585c697b4
# MAGIC %md
# MAGIC
# MAGIC ## DataFramesとSQLの間で変換する

# COMMAND ----------

# DBTITLE 0,--i18n-0b6ceb09-86dc-4cdd-9721-496d01e8737f
# MAGIC %md
# MAGIC **`createOrReplaceTempView`** はDataFrameに基づいて一時的なビューを作成します。テンポラリビューの寿命は、DataFrameの作成に使用されたSparkSessionに紐づいています。

# COMMAND ----------

budget_df.createOrReplaceTempView("budget")

# COMMAND ----------

display(spark.sql("SELECT * FROM budget"))

# COMMAND ----------

# DBTITLE 0,--i18n-81ff52ca-2160-4bfd-a78f-b3ba2f8b4933
# MAGIC %md
# MAGIC
# MAGIC 以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除します。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
