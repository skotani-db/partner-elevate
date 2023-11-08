-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-a51f84ef-37b4-4341-a3cc-b85c491339a8
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC # Spark SQL における高階関数
-- MAGIC
-- MAGIC Spark SQL における高階関数を使用すると、配列やマップ型の複雑なデータ型を変換することができ、その元の構造を保持したまま変更できます。例には次のようなものがあります。
-- MAGIC - **`FILTER()`** : 与えられたラムダ関数を使用して配列をフィルタリングします。
-- MAGIC - **`EXIST()`** : ステートメントが配列内の1つ以上の要素に対して真であるかどうかをテストします。
-- MAGIC - **`TRANSFORM()`** : 与えられたラムダ関数を使用して配列内のすべての要素を変換します。
-- MAGIC - **`REDUCE()`** : 2つのラムダ関数を取り、配列の要素をバッファにマージして最終的なバッファに対してフィニッシュ関数を適用して、配列の要素を単一の値に削減します。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このレッスンの終わりには、以下のことができるようになるはずです：
-- MAGIC * 高階関数を使用して配列を操作する
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-b295e5de-82bb-41c0-a470-2d8c6bbacc09
-- MAGIC %md
-- MAGIC
-- MAGIC ## セットアップを実行
-- MAGIC 以下のセルを実行して、環境をセットアップしてください。
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.99

-- COMMAND ----------

-- DBTITLE 0,--i18n-bc1d8e11-d1ff-4aa0-b4e9-3c2703826cd1
-- MAGIC %md
-- MAGIC ## フィルター
-- MAGIC **`FILTER`** 関数を使用して、提供された条件に基づいて各配列から値を除外した新しい列を作成できます。
-- MAGIC これを使用して、 **`sales`** データセットのすべてのレコードからキングサイズでない製品を削除するために、 **`items`** 列から製品を削除しましょう。
-- MAGIC
-- MAGIC **`FILTER (items, i -> i.item_id LIKE "%K") AS king_items`**
-- MAGIC
-- MAGIC 上記のステートメントでは：
-- MAGIC - **`FILTER`** : ハイアーオーダー関数の名前 <br>
-- MAGIC - **`items`** : 入力配列の名前 <br>
-- MAGIC - **`i`** : イテレータ変数の名前。この名前を選択し、それをラムダ関数内で使用します。これにより、配列を反復処理し、各値が一度に関数にサイクリングされます。<br>
-- MAGIC - **`->`** : 関数の開始を示します <br>
-- MAGIC - **`i.item_id LIKE "%K"`** : これが関数です。各値は大文字のKで終わるかどうかをチェックします。終わっている場合、新しい列 **`king_items`** にフィルタリングされます。
-- MAGIC
-- MAGIC **注意：**作成された列で多くの空の配列を生成するフィルターを記述することがあります。その場合、返される列で空でない配列値のみを表示するために **`WHERE`** 句を使用することが役立ちます。
-- MAGIC

-- COMMAND ----------

SELECT * FROM (
  SELECT
    order_id,
    FILTER (items, i -> i.item_id LIKE "%K") AS king_items
  FROM sales)
WHERE size(king_items) > 0

-- COMMAND ----------

-- DBTITLE 0,--i18n-3e2f5be3-1f8b-4a54-9556-dd72c3699a21
-- MAGIC %md
-- MAGIC
-- MAGIC ## 変換
-- MAGIC
-- MAGIC **`TRANSFORM()`** ハイアーオーダー関数は、配列内の各要素に既存の関数を適用する場合に特に有用です。
-- MAGIC これを使用して、 **`items`** 配列列内に含まれる要素を変換して新しい配列列 **`item_revenues`** を作成しましょう。
-- MAGIC
-- MAGIC 以下のクエリでは、 **`items`** は入力配列の名前であり、 **`i`** はイテレータ変数の名前です（この名前を選択し、それをラムダ関数内で使用します。配列を反復処理し、各値が一度に関数にサイクリングされます）、 **`->`** は関数の開始を示します。
-- MAGIC

-- COMMAND ----------

SELECT *,
  TRANSFORM (
    items, i -> CAST(i.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM sales

-- COMMAND ----------

-- DBTITLE 0,--i18n-ccfac343-4884-497a-a759-fc14b1666d6b
-- MAGIC %md
-- MAGIC
-- MAGIC 上記で指定したラムダ関数は、各値の **`item_revenue_in_usd`** サブフィールドを取得し、それを100倍に乗算し、整数にキャストして、その結果を新しい配列列 **`item_revenues`** に含めます。
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-9a5d0a06-c033-4541-b06e-4661804bf3c5
-- MAGIC %md
-- MAGIC
-- MAGIC ## Exists Lab
-- MAGIC ここでは、 **`sales`** テーブルのデータを使用して、マットレス製品または枕製品が購入されたかどうかを示すブール列  **`mattress`** と **`pillow`** を作成するために、高階関数 **`EXISTS`** を使用します。
-- MAGIC
-- MAGIC たとえば、 **`items`** 列の **`item_name`** が文字列 **`"Mattress"`** で終わる場合、 **`mattress`** 列の値は **`true`** で、 **`pillow`** 列の値は **`false`** である必要があります。以下はアイテムの例とその結果の値です。
-- MAGIC
-- MAGIC |  items  | mattress | pillow |
-- MAGIC | ------- | -------- | ------ |
-- MAGIC | **`[{..., "item_id": "M_PREM_K", "item_name": "Premium King Mattress", ...}]`** | true | false |
-- MAGIC | **`[{..., "item_id": "P_FOAM_S", "item_name": "Standard Foam Pillow", ...}]`** | false | true |
-- MAGIC | **`[{..., "item_id": "M_STAN_F", "item_name": "Standard Full Mattress", ...}]`** | true | false |
-- MAGIC
-- MAGIC <a href="https://docs.databricks.com/sql/language-manual/functions/exists.html" target="_blank">exists</a> 関数のドキュメントを参照してください。  
-- MAGIC 文字列 **`item_name`** が "Mattress" で終わるかどうかを確認するには、条件式 **`item_name LIKE "%Mattress"`** を使用できます。
-- MAGIC

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TABLE sales_product_flags AS
<FILL_IN>
EXISTS <FILL_IN>.item_name LIKE "%Mattress"
EXISTS <FILL_IN>.item_name LIKE "%Pillow"

-- COMMAND ----------

-- DBTITLE 0,--i18n-3dbc22b0-1092-40c9-a6cb-76ed364a4aae
-- MAGIC %md
-- MAGIC
-- MAGIC 以下のヘルパー関数は、指示に従っていない場合に、変更が必要な内容に関するエラーメッセージを返します。出力がない場合、このステップが完了していることを意味します。
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- DBTITLE 0,--i18n-caed8962-3717-4931-8ed2-910caf97740a
-- MAGIC %md
-- MAGIC
-- MAGIC 以下のセルを実行して、テーブルが正しく作成されたことを確認してください。
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("sales_product_flags", 10510, ['items', 'mattress', 'pillow'])
-- MAGIC product_counts = spark.sql("SELECT sum(CAST(mattress AS INT)) num_mattress, sum(CAST(pillow AS INT)) num_pillow FROM sales_product_flags").first().asDict()
-- MAGIC assert product_counts == {'num_mattress': 9986, 'num_pillow': 1384}, "There should be 9986 rows where mattress is true, and 1384 where pillow is true"

-- COMMAND ----------

-- DBTITLE 0,--i18n-ffcde68f-163a-4a25-85d1-c5027c664985
-- MAGIC %md
-- MAGIC
-- MAGIC 以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除します。
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
