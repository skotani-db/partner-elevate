# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-8c690200-3a21-4838-8d2a-15385fda557f
# MAGIC %md
# MAGIC # DLT Python構文の基本
# MAGIC
# MAGIC このノートブックでは、Delta Live Tables (DLT) を使用して、クラウドオブジェクトストレージに着信したJSONファイルの生データを処理し、データレイクハウスで分析ワークロードを実行する方法を示します。ここでは、メダリオンアーキテクチャを示しており、データがパイプラインを流れるにつれて段階的に変換およびエンリッチされる仕組みです。このノートブックは、このアーキテクチャではなく、DLTのPython構文に焦点を当てていますが、設計の簡単な概要も以下に示します。
# MAGIC
# MAGIC * ブロンズテーブルには、JSONから読み込まれた生のレコードが含まれており、レコードの取り込み方法に関するデータでエンリッチされています。
# MAGIC * シルバーテーブルは、対象のフィールドを検証およびエンリッチします。
# MAGIC * ゴールドテーブルには、ビジネスインサイトとダッシュボード作成のための集計データが含まれています。
# MAGIC
# MAGIC ## 学習目標
# MAGIC
# MAGIC このノートブックの終わりまでに、学生は以下について快適に感じるはずです：
# MAGIC * Delta Live Tablesの宣言
# MAGIC * Auto Loaderを使用してデータを取り込む
# MAGIC * DLTパイプラインでパラメータを使用する
# MAGIC * 制約を使用してデータ品質を強化する
# MAGIC * テーブルにコメントを追加する
# MAGIC * ライブテーブルとストリーミングライブテーブルの構文と実行の違いを説明する

# COMMAND ----------

# DBTITLE 0,--i18n-c23e126e-c267-4704-bfca-caee48728551
# MAGIC %md
# MAGIC ## DLTライブラリノートブックについて
# MAGIC
# MAGIC DLT構文はノートブック内での対話的な実行を意図していません。このノートブックは、適切に実行するためにDLTパイプラインの一部としてスケジュールする必要があります。
# MAGIC
# MAGIC このノートブックが作成された時点では、現在のDatabricksランタイムには **`dlt`** モジュールが含まれていないため、ノートブックでDLTコマンドを実行しようとすると失敗します。
# MAGIC
# MAGIC DLTコードの開発とトラブルシューティングについては、後のコースで詳しく説明します。
# MAGIC
# MAGIC ## パラメータ化
# MAGIC
# MAGIC DLTパイプラインの構成中に、いくつかのオプションが指定されました。そのうちの1つは、**Configurations** フィールドに追加されたキーと値のペアです。
# MAGIC
# MAGIC DLTパイプラインの構成は、Databricksジョブのパラメータに似ていますが、実際にはSpark設定として設定されています。
# MAGIC
# MAGIC Pythonでは、これらの値には **`spark.conf.get()`** を使用してアクセスできます。
# MAGIC
# MAGIC これらのレッスンでは、ノートブックの初めでPython変数 **`source`** を設定し、その後必要に応じてこの変数をコード内で使用します。
# MAGIC
# MAGIC ## インポートに関する注意
# MAGIC
# MAGIC **`dlt`** モジュールはPythonノートブックライブラリに明示的にインポートする必要があります。
# MAGIC
# MAGIC ここでは、 **`pyspark.sql.functions`** を **`F`** としてインポートする必要があります。
# MAGIC
# MAGIC 一部の開発者は **`*`** をインポートする一方、他の開発者は現在のノートブックで必要な関数のみをインポートすることがあります。
# MAGIC
# MAGIC これらのレッスンでは、 **`F`** を使用して、学生がこのライブラリからインポートされたメソッドが明確にわかるようにします。

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# DBTITLE 0,--i18n-d260ca6c-11c2-464d-8c16-f5ff6e56b8b0
# MAGIC %md
# MAGIC ## データフレームとしてのテーブル
# MAGIC
# MAGIC DLTで作成できる永続的なテーブルには2つの異なるタイプがあります：
# MAGIC * **ライブテーブル** はレイクハウスのためのマテリアライズドビューで、各リフレッシュごとにクエリの現在の結果を返します
# MAGIC * **ストリーミングライブテーブル** は増分的でほぼリアルタイムのデータ処理を行うために設計されています
# MAGIC
# MAGIC これらのオブジェクトは、両方ともDelta Lakeプロトコルで格納されるテーブルとして永続化されています（ACIDトランザクション、バージョニング、その他多くの利点を提供）。
# MAGIC
# MAGIC ライブテーブルとストリーミングライブテーブルの違いについては、ノートブックの後半で詳しく説明します。
# MAGIC
# MAGIC Delta Live Tablesは、おなじみのPySpark APIを拡張する多くの新しいPython関数を導入しています。
# MAGIC
# MAGIC この設計の中心にあるのは、 **`@dlt.table`** デコレーターです。これは、Spark DataFrameを返す任意のPython関数に追加されます。（**注意**：これにはKoalas DataFramesも含まれますが、このコースではカバーされません。）
# MAGIC
# MAGIC Sparkと/またはStructured Streamingで作業に慣れている場合、DLTで使用される構文の大部分は認識するでしょう。大きな違いは、DataFrameライターのメソッドやオプションを見ることは決してないことです。なぜなら、このロジックはDLTによって処理されるからです。
# MAGIC
# MAGIC そのため、DLTテーブルの基本的な形式は以下のようになります：
# MAGIC
# MAGIC **`@dlt.table`**<br/>
# MAGIC **`def <function-name>():`**<br/>
# MAGIC **`    return (<query>)`**</br>

# COMMAND ----------

# DBTITLE 0,--i18n-969f77f0-34bf-4e23-bdc5-1f0575e99ed1
# MAGIC %md
# MAGIC ## Auto Loaderを使用したストリーミング取り込み
# MAGIC
# MAGIC Databricksは、クラウドオブジェクトストレージからDelta Lakeにデータを増分的にロードするための最適化された実行を提供するために、[Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html) 機能を開発しました。DLTと組み合わせてAuto Loaderを使用するのは簡単です：ソースデータディレクトリを設定し、いくつかの設定を提供し、ソースデータに対するクエリを書くだけです。Auto Loaderは、新しいデータファイルがソースクラウドオブジェクトストレージの場所に着信するたびに、新しいレコードを増分的に処理し、高価なスキャンや無限に増加するデータセットの結果を再計算する必要はありません。
# MAGIC
# MAGIC Auto Loaderは、クラウドファイルのフォーマットとして **`format("cloudFiles")`** 設定を構成することで、Databricks全体で増分的なデータインジェクションを実行するためにStructured Streaming APIと組み合わせることができます。DLTでは、データの読み取りに関連する設定のみを構成します。スキーマの推論と進化の場所も、それらの設定が有効になっている場合、自動的に構成されます。
# MAGIC
# MAGIC 以下のクエリは、Auto Loaderで構成されたソースからのストリーミングDataFrameを返します。
# MAGIC
# MAGIC フォーマットとして **`cloudFiles`** を指定する他に、次のことを指定しています：
# MAGIC * オプション **`cloudFiles.format`** を **`json`** として指定します（これはクラウドオブジェクトストレージのファイルのフォーマットを示します）
# MAGIC * オプション **`cloudFiles.inferColumnTypes`** を **`True`** として指定します（各列の型を自動検出するため）
# MAGIC * クラウドオブジェクトストレージへのパスを **`load`** メソッドに指定します
# MAGIC * ソースフィールドに沿ってデータをエンリッチするいくつかの **`pyspark.sql.functions`** を含むselectステートメント
# MAGIC
# MAGIC デフォルトでは、 **`@dlt.table`** はターゲットテーブルの名前として関数の名前を使用します。

# COMMAND ----------

@dlt.table
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/orders")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-d4d6be28-67fb-44d8-ab4a-cfae62d819ed
# MAGIC %md
# MAGIC ## データの検証、エンリッチ、変換
# MAGIC
# MAGIC DLTを使用すると、標準的なSpark変換の結果からテーブルを簡単に宣言できます。DLTはデータ品質チェックの新しい機能を追加し、作成されたテーブルのメタデータをエンリッチするための多くのオプションを提供します。
# MAGIC
# MAGIC 以下のクエリの構文を詳しく見てみましょう。
# MAGIC
# MAGIC ### **`@dlt.table()`** のオプション
# MAGIC
# MAGIC テーブルの作成時に指定できる<a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-python-ref.html#create-table" target="_blank">オプション</a>がいくつかあります。ここでは、データセットを注釈付けするために2つのオプションを使用します。
# MAGIC
# MAGIC ##### **`comment`**
# MAGIC
# MAGIC テーブルコメントはリレーショナルデータベースの標準です。これらは、組織全体のユーザーに有用な情報を提供するために使用できます。この例では、テーブルの短い人間が読める説明を記述し、データの取り込みと強制がどのように行われているかを説明しています（他のテーブルのメタデータを確認することでも得られる情報です）。
# MAGIC
# MAGIC ##### **`table_properties`**
# MAGIC
# MAGIC このフィールドを使用して、データのカスタムタグ付けに任意のキー/値ペアを渡すことができます。ここでは、キー **`quality`** に対して値 **`silver`** を設定します。
# MAGIC
# MAGIC このフィールドはカスタムタグを任意に設定できる一方、テーブルの動作を制御する設定の数も構成に使用されます。テーブルの詳細を確認する際に、テーブルが作成されるたびにデフォルトでオンになる設定もいくつか見つかるかもしれません。
# MAGIC
# MAGIC ### データ品質制約
# MAGIC
# MAGIC PythonバージョンのDLTは、<a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html#delta-live-tables-data-quality-constraints" target="_blank">データ品質制約</a>を設定するためにデコレータ関数を使用します。このコースでは、いくつかの制約を見ていきます。
# MAGIC
# MAGIC DLTは、データの品質強制チェックに対する単純なブール式を使用しています。以下のステートメントでは、以下のことを実行します：
# MAGIC * **`valid_date`** という名前の制約を宣言します
# MAGIC * 条件チェックを定義し、フィールド **`order_timestamp`** が2021年1月1日よりも大きい値を含む必要があると指定します
# MAGIC * デコレータ **`@dlt.expect_or_fail()`** を使用して、制約を違反するレコードがある場合、現在のトランザクションを失敗させるようにDLTに指示します
# MAGIC
# MAGIC 各制約には複数の条件を持たせることができ、1つのテーブルに複数の制約を設定できます。制約違反は更新を失敗させるだけでなく、レコードを自動的に削除したり、無効なレコードを処理しながら違反の数を記録したりすることもできます。
# MAGIC
# MAGIC ### DLT読み込みメソッド
# MAGIC
# MAGIC Pythonの **`dlt`** モジュールは、DLTパイプライン内で他のテーブルとビューへの参照を簡単に構成するための **`read()`** および **`read_stream()`** メソッドを提供します。この構文を使用すると、データベ
# MAGIC
# MAGIC ースの参照なしでこれらのデータセットを名前で参照できます。また、DLTパイプラインで参照されているデータベースを置換キーワード **`LIVE`** として使用することもできます。

# COMMAND ----------

@dlt.table(
    comment = "Append only orders with valid timestamps",
    table_properties = {"quality": "silver"})
@dlt.expect_or_fail("valid_date", F.col("order_timestamp") > "2021-01-01")
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
            .select(
                "processing_time",
                "customer_id",
                "notifications",
                "order_id",
                F.col("order_timestamp").cast("timestamp").alias("order_timestamp")
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-e6fed8ba-7028-4d19-9310-cc705b7858e4
# MAGIC %md
# MAGIC ## Live Tables vs. Streaming Live Tables（ライブテーブル vs. ストリーミングライブテーブル）
# MAGIC
# MAGIC これまでに見てきた2つの関数は、いずれもストリーミングライブテーブルを作成しました。以下では、いくつかの集計データのライブテーブル（またはマテリアライズドビュー）を返すシンプルな関数を見てみます。
# MAGIC
# MAGIC Sparkは、バッチクエリとストリーミングクエリを過去に区別してきました。ライブテーブルとストリーミングライブテーブルにも似たような違いがあります。
# MAGIC
# MAGIC これらのテーブルタイプは、PySparkおよびStructured Streaming APIの構文（およびいくつかの制限）を継承していることに注意してください。
# MAGIC
# MAGIC 以下は、これらのタイプのテーブルの違いのいくつかです。
# MAGIC
# MAGIC ### ライブテーブル
# MAGIC * 常に「正確」であり、更新後にその内容がその定義と一致します。
# MAGIC * すべてのデータに対して初めて定義されたかのように同じ結果を返します。
# MAGIC * DLTパイプラインの外部での操作によって変更されるべきではありません（未定義の回答を受けるか、変更が元に戻される可能性があります）。
# MAGIC
# MAGIC ### ストリーミングライブテーブル
# MAGIC * "追記のみ"のストリーミングソースからの読み取りのみをサポートします。
# MAGIC * 入力バッチごとに1回だけ読み取ります（対象ディメンションが変更された場合、クエリの定義が変更された場合など、関係なく）。
# MAGIC * 管理されていないDLTパイプラインの外でテーブルに操作を実行できます（データを追加、GDPRを実行など）。

# COMMAND ----------

@dlt.table
def orders_by_date():
    return (
        dlt.read("orders_silver")
            .groupBy(F.col("order_timestamp").cast("date").alias("order_date"))
            .agg(F.count("*").alias("total_daily_orders"))
    )

# COMMAND ----------

# DBTITLE 0,--i18n-facc7d78-16e4-4f03-a232-bb5e4036952a
# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックを見て、以下の内容について快適に感じるはずです：
# MAGIC * Delta Live Tablesの宣言
# MAGIC * Auto Loaderを使用したデータの取り込み
# MAGIC * DLTパイプラインでのパラメータの使用
# MAGIC * 制約によるデータ品質の強制
# MAGIC * テーブルにコメントを追加
# MAGIC * ライブテーブルとストリーミングライブテーブルの構文と実行の違いの説明
# MAGIC
# MAGIC 次のノートブックでは、これらの構文構造についてさらに学びながら、新しいコンセプトをいくつか追加します。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
