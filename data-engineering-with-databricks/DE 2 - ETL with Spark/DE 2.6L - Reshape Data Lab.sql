-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-af5bea55-ebfc-4d31-a91f-5d6cae2bc270
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # データ再形成ラボ
-- MAGIC
-- MAGIC このラボでは、 **`events`** でユーザーが特定のアクションを何回実行したかを集計する **`clickpaths`** テーブルを作成し、その情報を **`transactions`** のフラットビューと結合して、各ユーザーのアクションと最終的な購入の記録を作成します。
-- MAGIC
-- MAGIC **`clickpaths`** テーブルには、 **`transactions`** からのすべてのフィールドと、 **`events`** からの各 **`event_name`** の数を含む列が含まれるべきです。このテーブルは、購入を完了した各ユーザーに対して単一の行を持つべきです。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このラボの最後までに、以下のことができるようになるべきです：
-- MAGIC - ピボットおよびテーブルを結合して、各ユーザーのクリックパスを作成する

-- COMMAND ----------

-- DBTITLE 0,--i18n-5258fa9b-065e-466d-9983-89f0be627186
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## セットアップ実行
-- MAGIC
-- MAGIC セットアップスクリプトはデータを作成し、このノートブックの残りの部分を実行するために必要な値を宣言します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.6L

-- COMMAND ----------

-- DBTITLE 0,--i18n-082bfa19-8e4e-49f7-bd5d-cd833c471109
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC このラボでは、Pythonを使って時々チェックを行います。以下のヘルパー関数は、指示に従わなかった場合、変更すべき点のメッセージとともにエラーを返します。出力がない場合は、このステップを完了したことを意味します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- DBTITLE 0,--i18n-2799ea4f-4a8e-4ad4-8dc1-a8c1f807c6d7
-- MAGIC %md
-- MAGIC
-- MAGIC ## 各ユーザーのイベントカウントを得るためにイベントをピボットする
-- MAGIC
-- MAGIC まずは　**`events`**　テーブルをピボットして、各　**`event_name`**　のカウントを取得することから始めましょう。
-- MAGIC
-- MAGIC 各ユーザーが **`event_name`** 列で指定された特定のイベントを実行した回数を集計したいと思います。これを行うには、　**`user_id`**　でグループ化し、　**`event_name`**　でピボットを行い、各イベントタイプのカウントを独自のカラムで提供します。　**`user_id`**　はターゲットスキーマでは　**`user`**　にリネームされることに注意してください。
-- MAGIC
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | pillows | BIGINT |
-- MAGIC | login | BIGINT |
-- MAGIC | main | BIGINT |
-- MAGIC | careers | BIGINT |
-- MAGIC | guest | BIGINT |
-- MAGIC | faq | BIGINT |
-- MAGIC | down | BIGINT |
-- MAGIC | warranty | BIGINT |
-- MAGIC | finalize | BIGINT |
-- MAGIC | register | BIGINT |
-- MAGIC | shipping_info | BIGINT |
-- MAGIC | checkout | BIGINT |
-- MAGIC | mattresses | BIGINT |
-- MAGIC | add_item | BIGINT |
-- MAGIC | press | BIGINT |
-- MAGIC | email_coupon | BIGINT |
-- MAGIC | cc_info | BIGINT |
-- MAGIC | foam | BIGINT |
-- MAGIC | reviews | BIGINT |
-- MAGIC | original | BIGINT |
-- MAGIC | delivery | BIGINT |
-- MAGIC | premium | BIGINT |
-- MAGIC
-- MAGIC イベント名のリストは以下のTODOセルに記載されている。

-- COMMAND ----------

-- DBTITLE 0,--i18n-0d995af9-e6f3-47b0-8b78-44bda953fa37
-- MAGIC %md
-- MAGIC
-- MAGIC ### SQLで解く

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW events_pivot
<FILL_IN>
("cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
"register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
"cc_info", "foam", "reviews", "original", "delivery", "premium")

-- COMMAND ----------

-- DBTITLE 0,--i18n-afd696e4-049d-47e1-b266-60c7310b169a
-- MAGIC %md
-- MAGIC
-- MAGIC ### Pythonで解く

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC (spark.read
-- MAGIC     <FILL_IN>
-- MAGIC     .createOrReplaceTempView("events_pivot"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-2fe0f24b-2364-40a3-9656-c124d6515c4d
-- MAGIC %md
-- MAGIC
-- MAGIC ### 作業の確認
-- MAGIC 以下のセルを実行して、ビューが正しく作成されたことを確認してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("events_pivot", 204586, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium'])

-- COMMAND ----------

-- DBTITLE 0,--i18n-eaac4506-501a-436a-b2f3-3788c689e841
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## イベント数とトランザクションをすべてのユーザーに結合
-- MAGIC
-- MAGIC 次に、events_pivot を transactions と結合して、テーブル clickpaths を作成します。このテーブルには、上記で作成した events_pivot テーブルからの同じイベント名の列が続き、以下に示すように transactions テーブルからの列が続きます。
-- MAGIC
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | ... | ... |
-- MAGIC | user_id | STRING |
-- MAGIC | order_id | BIGINT |
-- MAGIC | transaction_timestamp | BIGINT |
-- MAGIC | total_item_quantity | BIGINT |
-- MAGIC | purchase_revenue_in_usd | DOUBLE |
-- MAGIC | unique_items | BIGINT |
-- MAGIC | P_FOAM_K | BIGINT |
-- MAGIC | M_STAN_Q | BIGINT |
-- MAGIC | P_FOAM_S | BIGINT |
-- MAGIC | M_PREM_Q | BIGINT |
-- MAGIC | M_STAN_F | BIGINT |
-- MAGIC | M_STAN_T | BIGINT |
-- MAGIC | M_PREM_K | BIGINT |
-- MAGIC | M_PREM_F | BIGINT |
-- MAGIC | M_STAN_K | BIGINT |
-- MAGIC | M_PREM_T | BIGINT |
-- MAGIC | P_DOWN_S | BIGINT |
-- MAGIC | P_DOWN_K | BIGINT |

-- COMMAND ----------

-- DBTITLE 0,--i18n-03571117-301e-4c35-849a-784621656a83
-- MAGIC %md
-- MAGIC
-- MAGIC ### SQLで解く

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW clickpaths AS
<FILL_IN>

-- COMMAND ----------

-- DBTITLE 0,--i18n-e0ad84c7-93f0-4448-9846-4b56ba71acf8
-- MAGIC %md
-- MAGIC
-- MAGIC ### Pythonで解く

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC (spark.read
-- MAGIC     <FILL_IN>
-- MAGIC     .createOrReplaceTempView("clickpaths"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-ac19c8e1-0ab9-4558-a0eb-a6c954e84167
-- MAGIC %md
-- MAGIC
-- MAGIC ### 作業の確認
-- MAGIC 以下のセルを実行して、ビューが正しく作成されたことを確認してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("clickpaths", 9085, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium', 'user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K'])

-- COMMAND ----------

-- DBTITLE 0,--i18n-f352b51d-72ce-48d9-9944-a8f4c0a2a5ce
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
