-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-c457f8c6-b318-46da-a68f-36f67d8a1b9c
-- MAGIC %md
-- MAGIC # DLT SQL構文の基本
-- MAGIC
-- MAGIC このノートブックでは、Delta Live Tables（DLT）を使用して、クラウドオブジェクトストレージに着信するJSONファイルからの生データを処理し、データレイクハウスで分析ワークロードを実行する一連のテーブルを作成します。ここでは、データがパイプラインを介してフローする際に段階的に変換および拡張されるメダリオンアーキテクチャを示します。このノートブックでは、このアーキテクチャではなく、DLTのSQL構文に焦点を当てていますが、設計の概要も簡単に説明します。
-- MAGIC
-- MAGIC * ブロンズテーブルにはJSONから読み込まれ、レコードがどのように投入されたかを説明するデータで拡張された生のレコードが含まれています。
-- MAGIC * シルバーテーブルは、興味のあるフィールドの検証および拡張を行います。
-- MAGIC * ゴールドテーブルには、ビジネスインサイトおよびダッシュボードに駆動するための集計データが含まれています。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC
-- MAGIC このノートブックの最後までに、学生は次の点について自信を持つはずです：
-- MAGIC * Delta Live Tablesの宣言
-- MAGIC * Auto Loaderを使用したデータの取り込み
-- MAGIC * DLTパイプラインでのパラメータの使用
-- MAGIC * 制約を使用したデータ品質の強制
-- MAGIC * テーブルへのコメントの追加
-- MAGIC * ライブテーブルとストリーミングライブテーブルの構文と実行の違いの説明
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-736da23b-5e31-4b7c-9f58-b56f3a133714
-- MAGIC %md
-- MAGIC ## DLTライブラリノートブックについて
-- MAGIC
-- MAGIC DLTの構文は、ノートブックで対話的に実行するために意図されていません。このノートブックは、正確な実行を確認するためにはDLTパイプラインの一部としてスケジュールする必要があります。
-- MAGIC
-- MAGIC DLTノートブックセルを対話的に実行する場合、文法的には正しいとのメッセージが表示されるはずです。このメッセージが返される前に一部の構文チェックが行われますが、クエリが望むように機能することを保証するものではありません。DLTコードの開発とトラブルシューティングについては、後のコースで詳しく説明します。
-- MAGIC
-- MAGIC ## パラメータ化
-- MAGIC
-- MAGIC DLTパイプラインの構成中に、いくつかのオプションが指定されました。その中の1つが**Configurations**フィールドに追加されたキーと値のペアでした。
-- MAGIC DLTパイプラインの構成でのConfigurationsは、Databricks JobsのパラメータやDatabricksノートブックのウィジェットに類似しています。
-- MAGIC これらのレッスン全体で、SQLクエリ内で構成で設定されたファイルパスを含めるために **`${source}`** を使用します。

-- COMMAND ----------

-- DBTITLE 0,--i18n-e47d07ed-d184-4108-bc22-53fd34678a67
-- MAGIC %md
-- MAGIC ## クエリ結果としてのテーブル
-- MAGIC
-- MAGIC Delta Live Tablesは、標準のSQLクエリを適応して、DDL（データ定義言語）とDML（データ操作言語）を統一された宣言的な構文に統合します。
-- MAGIC
-- MAGIC DLTで作成できる2つの異なる種類の永続テーブルがあります：
-- MAGIC - **Liveテーブル**は、レイクハウスのためのマテリアライズドビューであり、各リフレッシュでクエリの現在の結果を返します。
-- MAGIC - **Streaming Liveテーブル**は、増分的でほぼリアルタイムのデータ処理を目的としています。
-- MAGIC
-- MAGIC これらのオブジェクトは、Delta Lakeプロトコルで保存される永続テーブルとして取り扱われます（ACIDトランザクション、バージョニングなどの多くの利点を提供します）。LiveテーブルとStreaming Liveテーブルの違いについては、後のノートブックで詳しく説明します。
-- MAGIC
-- MAGIC どちらのタイプのテーブルも、わずかに変更されたCTAS（create table as select）ステートメントのアプローチを採用しています。エンジニアはデータを変換するためのクエリを書くことだけに注意すれば、DLTが残りを処理します。
-- MAGIC
-- MAGIC SQL DLTクエリの基本的な構文は次のとおりです：
-- MAGIC
-- MAGIC **`CREATE OR REFRESH [STREAMING] LIVE TABLE table_name`**<br/>
-- MAGIC **`AS select_statement`**<br/>

-- COMMAND ----------

-- DBTITLE 0,--i18n-32b81589-fb23-45c0-8317-977a7d4c722a
-- MAGIC %md
-- MAGIC
-- MAGIC ## Auto Loaderを使用したStreaming Ingestion
-- MAGIC
-- MAGIC Databricksは、[Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)機能を開発して、クラウドオブジェクトストレージからDelta Lakeにデータを増分的にロードするための最適化された実行を提供しています。Auto LoaderをDLTと組み合わせるのは簡単です：ソースデータディレクトリを構成し、いくつかの設定を提供し、ソースデータに対してクエリを書きます。Auto Loaderは、新しいデータファイルがソースのクラウドオブジェクトストレージの場所に着信すると、それらを自動的に検出し、高価なスキャンや無限に増加するデータセットに対して結果を再計算する必要はありません。
-- MAGIC
-- MAGIC **`cloud_files()`** メソッドを使用すると、Auto LoaderをSQLとネイティブに組み合わせることができます。このメソッドは、次の位置パラメーターを取ります：
-- MAGIC * ソースの場所。これはクラウドベースのオブジェクトストレージである必要があります。
-- MAGIC * ソースデータの形式。この場合はJSONです。
-- MAGIC * オプションのリーダーオプションの任意のサイズのカンマ区切りのリスト。この場合、 **`cloudFiles.inferColumnTypes`** を **`true`** に設定します。
-- MAGIC
-- MAGIC 以下のクエリでは、ソースに含まれるフィールドに加えて、各レコードがいつインジェストされたかと、各レコードの特定のファイルソースに関する情報をキャプチャするために、Spark SQL関数 **`current_timestamp()`** および **`input_file_name()`** が使用されています。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_bronze
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("${source}/orders", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-4be3b288-fd3b-4380-b32a-29fdb0d499ac
-- MAGIC %md
-- MAGIC ## データの検証、拡張、変換
-- MAGIC
-- MAGIC DLTを使用すると、標準的なSpark変換の結果からテーブルを簡単に宣言できます。DLTはデータセットのドキュメント化に他の場所で使用されているSpark SQLの機能を活用しながら、データ品質のチェックのための新しい機能を追加します。
-- MAGIC
-- MAGIC 以下のクエリの構文を分解してみましょう。
-- MAGIC
-- MAGIC ### Select文
-- MAGIC
-- MAGIC select文にはクエリのコアロジックが含まれています。この例では、次のことを行っています。
-- MAGIC * フィールド **`order_timestamp`** をタイムスタンプ型にキャストします
-- MAGIC * すべての残りのフィールドを選択します（元の **`order_timestamp`** を含む、興味のない3つのフィールドを除く）
-- MAGIC
-- MAGIC **FROM**句には2つの構造があるかもしれませんが、これにはなれていないかもしれません：
-- MAGIC * **`LIVE`** キーワードは、スキーマ名の代わりに、現在のDLTパイプラインの構成されたターゲットスキーマを指すために使用されます
-- MAGIC * **`STREAM`** メソッドは、SQLクエリのストリーミングデータソースを宣言するために使用されます
-- MAGIC
-- MAGIC なお、パイプラインの構成時にターゲットスキーマが宣言されていない場合、テーブルは公開されません（つまり、メタストアに登録されず、他の場所でクエリできるようにはなりません）。ターゲットスキーマは、異なる実行環境間を移動する際に簡単に変更できます。これにより、同じコードがリージョナルなワークロードに対して簡単に展開されたり、開発環境から本番環境に昇格したりする際に、スキーマ名をハードコードする必要がありません。
-- MAGIC
-- MAGIC ### データ品質の制約
-- MAGIC
-- MAGIC DLTは、データに品質の強制チェックを許可するためのシンプルなブール文を使用します。以下の文では、次のことを行います。
-- MAGIC * **`valid_date`** という名前の制約を宣言します
-- MAGIC * 条件付きチェックを定義します。 **`order_timestamp`** フィールドには、2021年1月1日よりも後の値が含まれている必要があります
-- MAGIC * 任意のレコードが制約を犯す場合、DLTにはトランザクションを失敗させるように指示します
-- MAGIC
-- MAGIC 各制約には複数の条件がある可能性があり、1つのテーブルに対して複数の制約を設定できます。更新に失敗するだけでなく、制約の違反は、これらの無効なレコードを処理しながら、自動的にレコードを削除するか、違反の数だけを記録することもできます。
-- MAGIC
-- MAGIC ### テーブルコメント
-- MAGIC
-- MAGIC テーブルコメントは標準的なSQLであり、組織全体で有用な情報を提供するために使用できます。この例では、テーブルに関するデータがどのようにインジェストされ、強制されているかについての簡単な人間が読める説明を書いています（これは他のテーブルメタデータを確認することでもわかるかもしれません）。
-- MAGIC
-- MAGIC ### テーブルプロパティ
-- MAGIC
-- MAGIC **`TBLPROPERTIES`**  フィールドは、データのカスタムタグ付けのためのキー/バリューペアを任意の数だけ渡すために使用できます。ここでは、 **`quality`** のキーに **`silver`** の値を設定しています。
-- MAGIC
-- MAGIC なお、このフィールドはカスタムタグを任意に設定できるようになっていますが、テーブルのパフォーマンスを制御するいくつかの設定を指定するためにも使用されます。テーブルの詳細を確認する際には、テーブルが作成されるたびにデフォルトでオンになるいくつかの設定にも遭遇するかもしれません。
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_silver
(CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION FAIL UPDATE)
COMMENT "Append only orders with valid timestamps"
TBLPROPERTIES ("quality" = "silver")
AS SELECT timestamp(order_timestamp) AS order_timestamp, * EXCEPT (order_timestamp, source_file, _rescued_data)
FROM STREAM(LIVE.orders_bronze)

-- COMMAND ----------

-- DBTITLE 0,--i18n-c88a50db-2c24-4117-be3f-193da33e4a5b
-- MAGIC %md
-- MAGIC ## Live Tables vs. Streaming Live Tables
-- MAGIC
-- MAGIC これまでに確認した2つのクエリは、どちらもストリーミングライブテーブルを作成しています。以下では、いくつかの集計データのライブテーブル（またはマテリアライズドビュー）を返す単純なクエリを見てみましょう。
-- MAGIC
-- MAGIC Sparkは歴史的に、バッチクエリとストリーミングクエリを区別してきました。ライブテーブルとストリーミングライブテーブルにも似たような違いがあります。
-- MAGIC
-- MAGIC ライブテーブルとストリーミングライブテーブルの唯一の構文の違いは、作成句での **`STREAMING`** キーワードの欠如と、ソーステーブルを **`STREAM()`** メソッドでラップしないことです。
-- MAGIC
-- MAGIC 以下は、これらのテーブルの種類の違いのいくつかです。
-- MAGIC
-- MAGIC ### ライブテーブル
-- MAGIC * 常に「正確」で、更新後にその内容が定義と一致します。
-- MAGIC * すべてのデータでまるでテーブルが初めて定義されたかのような結果を返します。
-- MAGIC * DLTパイプライン外の操作によって変更されるべきではありません（未定義の回答が得られるか、変更が元に戻されるかもしれません）。
-- MAGIC
-- MAGIC ### ストリーミングライブテーブル
-- MAGIC * 「追加のみ」のストリーミングソースからの読み取りのみサポートします。
-- MAGIC * 各入力バッチは一度だけ読み取り、それがどのように変更されても（結合されたディメンションが変更されても、クエリの定義が変更されてもなど）、一度だけです。
-- MAGIC * 管理対象のDLTパイプラインの外でテーブル上で操作を実行できます（データの追加、GDPRの実行など）。
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE orders_by_date
AS SELECT date(order_timestamp) AS order_date, count(*) AS total_daily_orders
FROM LIVE.orders_silver
GROUP BY date(order_timestamp)

-- COMMAND ----------

-- DBTITLE 0,--i18n-e15b4f61-b33a-4ac5-8b81-c6e7578ce28f
-- MAGIC %md
-- MAGIC ## 要約
-- MAGIC
-- MAGIC このノートブックを確認することで、次のことができるようになるはずです。
-- MAGIC * Delta Live Tablesの宣言
-- MAGIC * Auto Loaderを使用したデータの読み込み
-- MAGIC * DLTパイプラインでのパラメータの使用
-- MAGIC * 制約を使用したデータ品質の強制
-- MAGIC * テーブルへのコメントの追加
-- MAGIC * ライブテーブルとストリーミングライブテーブルの構文と実行の違いの説明
-- MAGIC
-- MAGIC 次のノートブックでは、これらの構文的な構造を学びながら、新しい概念もいくつか追加していきます。

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
