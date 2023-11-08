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
-- MAGIC # Reshaping Data Lab
-- MAGIC
-- MAGIC In this lab, you will create a **`clickpaths`** table that aggregates the number of times each user took a particular action in **`events`** and then join this information with a flattened view of **`transactions`** to create a record of each user's actions and final purchases.
-- MAGIC
-- MAGIC The **`clickpaths`** table should contain all the fields from **`transactions`**, as well as a count of every **`event_name`** from **`events`** in its own column. This table should contain a single row for each user that completed a purchase.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Pivot and join tables to create clickpaths for each user

-- COMMAND ----------

-- DBTITLE 0,--i18n-5258fa9b-065e-466d-9983-89f0be627186
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Run Setup
-- MAGIC
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.6L

-- COMMAND ----------

-- DBTITLE 0,--i18n-082bfa19-8e4e-49f7-bd5d-cd833c471109
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC We'll use Python to run checks occasionally throughout the lab. The helper functions below will return an error with a message on what needs to change if you have not followed instructions. No output means that you have completed this step.

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
-- MAGIC ## Pivot events to get event counts for each user
-- MAGIC
-- MAGIC Let's start by pivoting the **`events`** table to get counts for each **`event_name`**.
-- MAGIC
-- MAGIC We want to aggregate the number of times each user performed a specific event, specified in the **`event_name`** column. To do this, group by **`user_id`** and pivot on **`event_name`** to provide a count of every event type in its own column, resulting in the schema below. Note that **`user_id`** is renamed to **`user`** in the target schema.
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
-- MAGIC A list of the event names are provided in the TODO cells below.

-- COMMAND ----------

-- DBTITLE 0,--i18n-0d995af9-e6f3-47b0-8b78-44bda953fa37
-- MAGIC %md
-- MAGIC
-- MAGIC ### Solve with SQL

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
-- MAGIC ### Solve with Python

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
-- MAGIC ### Check your work
-- MAGIC Run the cell below to confirm the view was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("events_pivot", 204586, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium'])

-- COMMAND ----------

-- DBTITLE 0,--i18n-eaac4506-501a-436a-b2f3-3788c689e841
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Join event counts and transactions for all users
-- MAGIC
-- MAGIC Next, join **`events_pivot`** with **`transactions`** to create the table **`clickpaths`**. This table should have the same event name columns from the **`events_pivot`** table created above, followed by columns from the **`transactions`** table, as shown below.
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
-- MAGIC ### Solve with SQL

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW clickpaths AS
<FILL_IN>

-- COMMAND ----------

-- DBTITLE 0,--i18n-e0ad84c7-93f0-4448-9846-4b56ba71acf8
-- MAGIC %md
-- MAGIC
-- MAGIC ### Solve with Python

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
-- MAGIC ### Check your work
-- MAGIC Run the cell below to confirm the view was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("clickpaths", 9085, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium', 'user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K'])

-- COMMAND ----------

-- DBTITLE 0,--i18n-f352b51d-72ce-48d9-9944-a8f4c0a2a5ce
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
