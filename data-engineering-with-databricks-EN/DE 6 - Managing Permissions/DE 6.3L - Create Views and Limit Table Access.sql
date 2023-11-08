-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-20e4c711-c890-4ef6-bc9b-328f8550ed08
-- MAGIC %md
-- MAGIC # Create views and limit table access
-- MAGIC
-- MAGIC In this notebook you will learn how to:
-- MAGIC * Create views
-- MAGIC * Manage access to views
-- MAGIC * Use dynamic view features to restrict access to columns and rows within a table

-- COMMAND ----------

-- DBTITLE 0,--i18n-ede11fd8-42c7-4b13-863e-30eb9eee7fc3
-- MAGIC %md
-- MAGIC ## Set Up
-- MAGIC
-- MAGIC Run the following cells to perform some setup. In order to avoid conflicts in a shared training environment, this will create a uniquely named database exclusively for your use. This will also create an example table called **silver** within the Unity Catalog metatore.

-- COMMAND ----------

-- DBTITLE 0,--i18n-e7942e1d-097f-43a5-a1d7-d54af274b859
-- MAGIC %md
-- MAGIC Note: this notebook assumes a catalog named *main* in your Unity Catalog metastore. If you need to target a different catalog, edit the following notebook, **Classroom-Setup**, before proceeding.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-06.3

-- COMMAND ----------

-- DBTITLE 0,--i18n-ef87e152-8e6a-4fd9-8cc7-579545aa01f9
-- MAGIC %md
-- MAGIC Let's examine the contents of the **silver** table.
-- MAGIC
-- MAGIC Note: as part of the setup, a default catalog and database was selected so we only need to specify table or view names without any additional levels.

-- COMMAND ----------

SELECT * FROM silver.heartrate_device

-- COMMAND ----------

-- DBTITLE 0,--i18n-f9c4630d-c04d-4ec3-8c20-8afea4ec7e37
-- MAGIC %md
-- MAGIC ## Create gold view
-- MAGIC
-- MAGIC With a silver table in place, let's create a view that aggregates data from silver, presenting data suitable for the gold layer of a medallion architecture.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.heartrate_avgs AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM silver.heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time))

-- COMMAND ----------

-- DBTITLE 0,--i18n-2eb72987-961a-4020-b775-1e5e29c08a1a
-- MAGIC %md
-- MAGIC Let's examine the gold view.

-- COMMAND ----------

SELECT * FROM gold.heartrate_avgs

-- COMMAND ----------

-- DBTITLE 0,--i18n-f5ccd808-ffbd-4e06-8ca2-c03dd80ad8e2
-- MAGIC %md
-- MAGIC ## Grant access to view [optional]
-- MAGIC
-- MAGIC With a new view in place, let's allow users in the **account users** group to query it.
-- MAGIC
-- MAGIC Perform this section by uncommenting the code cells and running them in sequence. You will also be prompted to run some queries in Databricks SQL. To do this:
-- MAGIC
-- MAGIC 1. Open a new tab and go to Databricks SQL.
-- MAGIC 1. Create a SQL warehouse following the instructions in *Create SQL Warehouse in Unity Catalog*.
-- MAGIC 1. Prepare to enter queries as instructed below in that environment.

-- COMMAND ----------

-- SHOW GRANT ON VIEW gold.heartrate_avgs

-- COMMAND ----------

-- SHOW GRANT ON TABLE silver.heartrate_device

-- COMMAND ----------

-- DBTITLE 0,--i18n-9bf38439-6ad4-4db5-bb4f-fe70b8e4cfec
-- MAGIC %md
-- MAGIC ### Grant SELECT privilege on view
-- MAGIC
-- MAGIC The first requirement is to grant the **SELECT** privilege on the view to the **account users** group.

-- COMMAND ----------

-- GRANT SELECT ON VIEW gold.heartrate_avgs to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-fbb44902-4662-4d9a-ab11-462a2b07a665
-- MAGIC %md
-- MAGIC ### Grant USAGE privilege on catalog and database
-- MAGIC
-- MAGIC As with tables, **USAGE** privilege is also required on the catalog and database in order to query the view.

-- COMMAND ----------

-- GRANT USAGE ON CATALOG ${DA.catalog_name} TO `account users`;
-- GRANT USAGE ON DATABASE gold TO `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-cfe19914-4f92-47da-a250-2d350a39736f
-- MAGIC %md
-- MAGIC ### Query view as user
-- MAGIC
-- MAGIC With appropriate grants in place, attempt to query the view in the Databricks SQL environment.
-- MAGIC
-- MAGIC Run the following cell to output a query statement that reads from the view. Copy and paste the output into a new query within the SQL environment, and run the query.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM gold.heartrate_avgs")

-- COMMAND ----------

-- DBTITLE 0,--i18n-330f0448-6bef-42bc-930d-1716886c97fb
-- MAGIC %md
-- MAGIC Notice that the query succeeds and the output is identical to the output above, as expected.
-- MAGIC
-- MAGIC Now replace **`gold.heartrate_avgs`** with **`silver.heartrate_device`** and re-run the query. Notice that the query now fails. This is because the user does not have **SELECT** privilege on the **`silver.heartrate_device`** table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM silver.heartrate_device ")

-- COMMAND ----------

-- DBTITLE 0,--i18n-7c6916be-d36a-4655-b95c-94402222573f
-- MAGIC %md
-- MAGIC
-- MAGIC Recall though, that **`heartrate_avgs`** is a view that selects from **`heartrate_device`**. How then, can the query on **`heartrate_avgs`** succeed? Unity Catalog allows the query to pass because the *owner* of that view has **SELECT** privilege on **`silver.heartrate_device`**. This is an important property since it allows us to implement views that can filter or mask rows or columns of a table, without allowing direct access to the underlying table we are trying to protect. We will see this mechanism in action next.

-- COMMAND ----------

-- DBTITLE 0,--i18n-4ea26d47-eee0-43e9-867b-4b6913679e41
-- MAGIC %md
-- MAGIC ## Dynamic views
-- MAGIC
-- MAGIC Dynamic views allow us to configure fine-grained access control, including:
-- MAGIC * security at the level of columns or rows.
-- MAGIC * data masking.
-- MAGIC
-- MAGIC Access control is acheived through the use of functions within the definition of the view. These functions include:
-- MAGIC * **`current_user()`**: returns the current user’s email address
-- MAGIC * **`is_account_group_member()`**: returns TRUE if the current user is a member of the specified group
-- MAGIC
-- MAGIC Note: for legacy compatibility, there also exists the function **`is_member()`** which returns TRUE if the current user is a member of the specified workspace-level group. Avoid using this function when implementing dynamic views in Unity Catalog.

-- COMMAND ----------

-- DBTITLE 0,--i18n-b39146b7-362b-46db-9a59-a8c589e94392
-- MAGIC %md
-- MAGIC ### Restrict columns
-- MAGIC Let's apply **`is_account_group_member()`** to mask out columns containing PII for members of the **account users** group through **`CASE`** statements within the **`SELECT`**.
-- MAGIC
-- MAGIC Note: this is a simple example to align with the setup of this training environment. In a production system the preferable method would be to restrict rows for users who are *not* members of a specific group.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.heartrate_avgs AS
SELECT
  CASE WHEN
    is_account_group_member('account users') THEN 'REDACTED'
    ELSE mrn
  END AS mrn,
  CASE WHEN
    is_account_group_member('account users') THEN 'REDACTED'
    ELSE name
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM silver.heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)

-- COMMAND ----------

-- DBTITLE 0,--i18n-c5c4afac-2736-4a57-be57-55535654a227
-- MAGIC %md
-- MAGIC Now let's reissue the grant on the updated view.

-- COMMAND ----------

-- GRANT SELECT ON VIEW gold.heartrate_avgs to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-271fb0d5-34f1-435f-967e-4cc650facd01
-- MAGIC %md
-- MAGIC Let's query the view, which will yield unfiltered output (assuming the current user has not been added to the **analysts** group).

-- COMMAND ----------

SELECT * FROM gold.heartrate_avgs

-- COMMAND ----------

-- DBTITLE 0,--i18n-1754f589-d48f-499d-b352-3fa735305eb9
-- MAGIC %md
-- MAGIC Now re-run the query you ran earlier in the Databricks SQL environment (changing **`silver`** back to **`gold_dailyavg`**). Notice that the PII is now filtered. There is no way for members of this group to gain access to the PII since it is being protected by the view, and there is no direct access to the underlying table.

-- COMMAND ----------

-- DBTITLE 0,--i18n-f7759fba-c2f8-4471-96f7-aa1ea5a6a00b
-- MAGIC %md
-- MAGIC ### Restrict rows
-- MAGIC Let's now apply **`is_account_group_member()`** to filter out rows. In this case, we'll create a new gold view that returns timestamp and heartrate value, restricted for members of the **analysts** group, to rows whose device id is less than 30. Row filtering can by done by applying the conditional as a **`WHERE`** clause in the **`SELECT`**.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_allhr AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM silver.heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

-- GRANT SELECT ON VIEW gold_allhr to `account users`

-- COMMAND ----------

SELECT * FROM gold_allhr

-- COMMAND ----------

-- DBTITLE 0,--i18n-63255ec6-2317-455c-9665-e1c2069546f4
-- MAGIC %md
-- MAGIC Now re-run the query you ran earlier in the Databricks SQL environment (changing **`gold_dailyavg`** to **`gold_allhr`**). Notice that rows whose device ID is 30 or greater are omitted from the output.

-- COMMAND ----------

-- DBTITLE 0,--i18n-d11e2467-cd67-411e-bedd-4ebe55301985
-- MAGIC %md
-- MAGIC ### Data masking
-- MAGIC One final use case for dynamic views is to mask data; that is, allow a subset of data through, but transform it in a way such that the entirety of the masked field cannot be deduced.
-- MAGIC
-- MAGIC Here we blend the approach of row and column filtering to augment our row filtering view with data masking. But rather than replacing the entire column with the string **REDACTED**, we utilize SQL string manipulation functions to display the last two digits of the **mrn**, while masking out the rest.
-- MAGIC
-- MAGIC Depending on your needs, SQL provides a fairly comprehensive library of string manipulation functions that can be leveraged to mask data in a number of different ways; the approach shown below illustrates a simple example of this.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_allhr AS
SELECT
  CASE WHEN
    is_account_group_member('account users') THEN CONCAT("******", RIGHT(mrn, 2))
    ELSE mrn
  END AS mrn,
  time,
  device_id,
  heartrate
FROM silver.heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

-- GRANT SELECT ON VIEW gold_allhr to `account users`

-- COMMAND ----------

SELECT * FROM gold_allhr

-- COMMAND ----------

-- DBTITLE 0,--i18n-c379505f-ebad-474d-9dad-92fa8a7a7ee9
-- MAGIC %md
-- MAGIC Re-run the query against **gold_allhr** one last time in the Databricks SQL environment. Notice that, in addition to some rows being filtered, the **mrn** column is masked such that only the last two digits are displayed. This provides enough information to correlate records against known patients, but in and of itself does not divulge any PII.

-- COMMAND ----------

-- DBTITLE 0,--i18n-8828d381-084f-43fa-afc3-a5a2ba170aeb
-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC Run the following cell to remove assets that were used in this example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
