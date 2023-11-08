-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-5ec757b3-50cf-43ac-a74d-6902d3e18983
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # SQL UDFと制御フロー
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このセクションの最後までに、以下のことができるようになるべきです：
-- MAGIC - SQL UDFを定義および登録する
-- MAGIC - SQL UDFを共有するために使用されるセキュリティモデルを説明する
-- MAGIC - SQLコードでの **`CASE`** / **`WHEN`** 文の使用
-- MAGIC - SQL UDF内でのカスタム制御フローに **`CASE`** / **`WHEN`** 文を活用する

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd5d37b8-b720-4a88-a2cf-9b3c43f697eb
-- MAGIC %md
-- MAGIC
-- MAGIC ## セットアップの実行
-- MAGIC
-- MAGIC セットアップスクリプトを実行すると、データが作成され、このノートブックの残りの部分を実行するために必要な値が宣言されます。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.7A

-- COMMAND ----------

-- DBTITLE 0,--i18n-e8fa445b-db52-43c4-a649-9904526c6a04
-- MAGIC %md
-- MAGIC
-- MAGIC %md
-- MAGIC
-- MAGIC ## ユーザー定義関数
-- MAGIC
-- MAGIC Spark SQLのユーザー定義関数（UDF）を使用すると、カスタムSQLロジックをデータベース内の関数として登録できます。これにより、Databricks上でSQLを実行できる場所でこれらのメソッドを再利用できます。これらの関数はSQLでネイティブに登録され、大規模なデータセットにカスタムロジックを適用する際にSparkの最適化を維持します。
-- MAGIC
-- MAGIC 最小限、SQL UDFを作成するには関数名、オプションのパラメータ、返す型、およびカスタムロジックが必要です。
-- MAGIC
-- MAGIC 以下の例では、単純な関数 **`sale_announcement`** が **`item_name`** と **`item_price`** をパラメータとして受け取り、アイテムを元の価格の80％でセール中であることを発表する文字列を返します。
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
RETURNS STRING
RETURN concat("The ", item_name, " is on sale for $", round(item_price * 0.8, 0));

SELECT *, sale_announcement(name, price) AS message FROM item_lookup

-- COMMAND ----------

-- DBTITLE 0,--i18n-5a5dfa8f-f9e7-4b5f-b229-30bed4497009
-- MAGIC %md
-- MAGIC
-- MAGIC この関数は、Spark処理エンジン内で並列にカラムのすべての値に適用されることに注意してください。SQL UDFは、Databricks上で効率的に実行されるカスタムロジックを定義する効果的な方法です。

-- COMMAND ----------

-- DBTITLE 0,--i18n-f9735833-a4f3-4966-8739-eb351025dc28
-- MAGIC %md
-- MAGIC
-- MAGIC ## SQL UDFのスコープと権限
-- MAGIC SQLユーザー定義関数：
-- MAGIC - 実行環境間で永続化されます（ノートブック、DBSQLクエリ、ジョブを含むことがあります）。
-- MAGIC - メタストア内にオブジェクトとして存在し、データベース、テーブル、ビューと同じテーブルACLで管理されます。
-- MAGIC - SQL UDFを**作成**するには、カタログで **`USE CATALOG`** が必要で、スキーマで **`USE SCHEMA`** および **`CREATE FUNCTION`** が必要です。
-- MAGIC - SQL UDFを**使用**するには、カタログで **`USE CATALOG`** が必要で、スキーマで **`USE SCHEMA`** および関数で **`EXECUTE`** が必要です。
-- MAGIC
-- MAGIC **`DESCRIBE FUNCTION`** を使用して、関数が登録された場所や予想される入力および返される情報の基本情報を表示できます（**`DESCRIBE FUNCTION EXTENDED`** でさらに詳細な情報も表示できます）。
-- MAGIC

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED sale_announcement

-- COMMAND ----------

-- DBTITLE 0,--i18n-091c02b4-07b5-4b2c-8e1e-8cb561eed5a3
-- MAGIC %md
-- MAGIC
-- MAGIC **`Body`** フィールドに注意してください。これは、関数自体で使用されているSQLロジックを示しています。

-- COMMAND ----------

-- DBTITLE 0,--i18n-bf549dbc-edb7-465f-a310-0f5c04cfbe0a
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## シンプルな制御フロー関数
-- MAGIC
-- MAGIC SQL UDFと制御フローを **`CASE`** / **`WHEN`** 句の形で組み合わせることで、SQLワークロード内での制御フローの最適化された実行が提供されます。標準のSQL構文 **`CASE`** / **`WHEN`** は、テーブルの内容に基づいて複数の条件文を評価し、代替の結果を提供します。
-- MAGIC
-- MAGIC ここでは、この制御フローロジックを関数でラップする方法を示し、それをSQLを実行できる場所で再利用できるようにします。

-- COMMAND ----------

CREATE OR REPLACE FUNCTION item_preference(name STRING, price INT)
RETURNS STRING
RETURN CASE 
  WHEN name = "Standard Queen Mattress" THEN "This is my default mattress"
  WHEN name = "Premium Queen Mattress" THEN "This is my favorite mattress"
  WHEN price > 100 THEN concat("I'd wait until the ", name, " is on sale for $", round(price * 0.8, 0))
  ELSE concat("I don't need a ", name)
END;

SELECT *, item_preference(name, price) FROM item_lookup

-- COMMAND ----------

-- DBTITLE 0,--i18n-14f5f9df-d17e-4e6a-90b0-d22bbc4e1e10
-- MAGIC %md
-- MAGIC
-- MAGIC 提供されている例は単純ですが、これらの基本的な原則は、Spark SQLでネイティブ実行用のカスタム計算とロジックを追加するために使用できます。
-- MAGIC
-- MAGIC 特に、多くの定義済みプロシージャやカスタム定義の式を持つシステムからユーザーを移行する可能性がある企業にとって、SQL UDFは一握りのユーザーが一般的なレポートや分析クエリに必要な複雑なロジックを定義できるようにします。

-- COMMAND ----------

-- DBTITLE 0,--i18n-451ef10d-9e38-4b71-ad69-9c2ed74601b5
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC このレッスンに関連するテーブルとファイルを削除するには、次のセルを実行してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
