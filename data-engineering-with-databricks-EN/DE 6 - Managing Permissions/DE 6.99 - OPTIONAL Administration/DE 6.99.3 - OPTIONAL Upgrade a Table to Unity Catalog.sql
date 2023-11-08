-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-5d2baeeb-370f-413f-8eb2-5138b4bf046c
-- MAGIC %md
-- MAGIC # Upgrade a table to Unity Catalog
-- MAGIC
-- MAGIC In this notebook you will learn how to:
-- MAGIC * Migrate a table from the existing legacy Hive metastore to Unity Catalog
-- MAGIC * Create appropriate grants to enable others to access the table
-- MAGIC * Perform simple transformations on a table while migrating to Unity Catalog

-- COMMAND ----------

-- DBTITLE 0,--i18n-4111c8be-0a37-43fe-9003-be8c9751760b
-- MAGIC %md
-- MAGIC ## Set Up
-- MAGIC
-- MAGIC Run the following cells to perform some setup. In order to avoid conflicts in a shared training environment, this will create a uniquely named database exclusively for your use. This will also create an example source table called **movies** within the legacy Hive metatore.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-06.99.1

-- COMMAND ----------

-- DBTITLE 0,--i18n-96a90022-a7bb-40db-8429-30c31cbbf215
-- MAGIC %md
-- MAGIC Within the legacy Hive metastore local to this workspace, we now have a table called **movies**, residing in a the user-specific database outlined in the cell output from above. To make things easier, the database name is stored in a Hive variable named *DA.my_schema_name*. Let's preview the data stored in this table using that variable.
-- MAGIC
-- MAGIC We use this unique database name in the hive metastore to avoid potentially interfering with others in a shared training environment.

-- COMMAND ----------

SELECT * FROM hive_metastore.`${DA.my_schema_name}`.movies LIMIT 10

-- COMMAND ----------

-- DBTITLE 0,--i18n-7b2f4105-35f5-4a00-8a19-b4db0c11e045
-- MAGIC %md
-- MAGIC ## Set up destination
-- MAGIC
-- MAGIC With a source table in place, let's set up a destination in Unity Catalog to migrate our table to.

-- COMMAND ----------

-- DBTITLE 0,--i18n-24602317-c35b-4876-9f46-952df2c101cf
-- MAGIC %md
-- MAGIC ### Select Unity Catalog metastore for usage
-- MAGIC
-- MAGIC A catalog was created for you with a user-specific name, stored in the variable **DA.catalog_name**. Let's begin by selecting this catalog from the Unity Catalog metastore. This eliminates the need to have to specify a catalog in your table references.

-- COMMAND ----------

USE CATALOG `${DA.catalog_name}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-179e80a6-3a69-4506-89b0-c8b26d86fada
-- MAGIC %md
-- MAGIC ### Create and select database for usage
-- MAGIC
-- MAGIC Now that we're using a unique catalog, we don't need to worry about using a unique database to avoid interfering with others.
-- MAGIC
-- MAGIC Let's create a database called **bronze_datasets**.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS bronze_datasets

-- COMMAND ----------

-- DBTITLE 0,--i18n-df709457-d477-48eb-9758-54f4ecf17556
-- MAGIC %md
-- MAGIC Now let's select the newly created database to further simplify working with the destination table. Again, this step is not necessary but it will further simplify references to your upgraded tables.

-- COMMAND ----------

USE bronze_datasets

-- COMMAND ----------

-- DBTITLE 0,--i18n-adecfe89-ab61-4392-92ea-b3847d0e3d17
-- MAGIC %md
-- MAGIC ## Upgrade the table
-- MAGIC
-- MAGIC Copying the table boils down to a simple **CREATE TABLE AS SELECT** (CTAS) operation, using the three-level namespace to specify the source table. We do not need to specify the destination using three levels due to the **USE CATALOG** and **USE** statements run previously.
-- MAGIC
-- MAGIC Note: for large tables this operation can take time as all table data is copied.

-- COMMAND ----------

CREATE OR REPLACE TABLE movies
AS SELECT * FROM hive_metastore.`${DA.my_schema_name}`.movies

-- COMMAND ----------

-- DBTITLE 0,--i18n-64c49876-3551-4528-be7f-aa9973aac059
-- MAGIC %md
-- MAGIC The table is now copied, with the new table under Unity Catalog control. Let's quickly check to see any grants on the new table.

-- COMMAND ----------

SHOW GRANTS ON movies

-- COMMAND ----------

-- DBTITLE 0,--i18n-2ec10faa-827d-41c7-a697-de4c4ab5ba79
-- MAGIC %md
-- MAGIC Currently there are no grants.
-- MAGIC
-- MAGIC Now let's examine the grants on the original table. Uncomment the code in the following cell and run it.

-- COMMAND ----------

-- SHOW GRANTS ON hive_metastore.`${DA.my_schema_name}`.movies

-- COMMAND ----------

-- DBTITLE 0,--i18n-dc1999a4-7957-40ec-8a99-cf3cab5c3f77
-- MAGIC %md
-- MAGIC This gives an error since this table lives in the legacy metastore and we are not running on a cluster with legacy table access control enabled. This highlights a key benefit of Unity Catalog: no additional configuration is needed to acheive a secure solution. Unity Catalog is secure by default.

-- COMMAND ----------

-- DBTITLE 0,--i18n-bcd994a7-e48a-4c4c-9d8a-df65ea2577d4
-- MAGIC %md
-- MAGIC ## Grant access to table [optional]
-- MAGIC
-- MAGIC With a new table in place, let's allow users in the **analysts** group to read from it.
-- MAGIC
-- MAGIC Note that you can only perform this section if you followed along with the *Manage users and groups* exercise and created a Unity Catalog group named **analysts**.
-- MAGIC
-- MAGIC Perform this section by uncommenting the code cells and running them in sequence. You will also be prompted to run some queries as a secondary user. To do this:
-- MAGIC
-- MAGIC 1. Open a separate private browsing session and log in to Databricks SQL using the user id you created when performing *Manage users and groups*.
-- MAGIC 1. Create a SQL endpoint following the instructions in *Create SQL Endpoint in Unity Catalog*.
-- MAGIC 1. Prepare to enter queries as instructed below in that environment.

-- COMMAND ----------

-- DBTITLE 0,--i18n-1f594ac2-2b40-451a-a5da-3415c3fe7492
-- MAGIC %md
-- MAGIC ### Grant SELECT privilege on table
-- MAGIC
-- MAGIC The first requirement is to grant the **SELECT** privilege on the new table to the **analysts** group.

-- COMMAND ----------

-- GRANT SELECT ON TABLE movies to `analysts`

-- COMMAND ----------

-- DBTITLE 0,--i18n-1e6e597a-349b-4f83-9ee5-509d6d65db1f
-- MAGIC %md
-- MAGIC ### Grant USAGE privilege on database
-- MAGIC
-- MAGIC **USAGE** privilege is also required on the database.

-- COMMAND ----------

-- GRANT USAGE ON DATABASE `bronze_datasets` TO `analysts`

-- COMMAND ----------

-- DBTITLE 0,--i18n-76a94a64-a0f8-4ed6-914f-0081fe5e0527
-- MAGIC %md
-- MAGIC ### Access table as user
-- MAGIC
-- MAGIC With appropriate grants in place, attempt to read from the table in the Databricks SQL environment of your secondary user. 
-- MAGIC
-- MAGIC Run the following cell to output a query statement that reads from the newly created table. Copy and paste the output into a new query within the SQL environment of your secondary user, and run the query.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog_name}.bronze_datasets.movies")

-- COMMAND ----------

-- DBTITLE 0,--i18n-44e957a6-fa12-42d7-b4e3-fce1d9fad020
-- MAGIC %md
-- MAGIC
-- MAGIC ## Transform table while upgrading
-- MAGIC
-- MAGIC Migrating a table to Unity Catalog is itself a simple operation, but the overall move to Unity Catalog is a big one for any organization. It's a great time to closely consider your tables and schemas and whether they still address your organization's business requirements that may have changed over time.
-- MAGIC
-- MAGIC The example we saw earlier takes an exact copy of the source table. Since migrating a table is a simple **CREATE TABLE AS SELECT** operation, we can perform any transformations during the migration that can be performed with **SELECT**. For example, let's expand on the previous example to do the following tranformations:
-- MAGIC * Assign the name *idx* to the first column
-- MAGIC * Additionally select only the columns **title**, **year**, **budget** and **rating**
-- MAGIC * Convert **year** and **budget** to **INT** (replacing any instances of the string *NA* with 0)
-- MAGIC * Convert **rating** to **DOUBLE**

-- COMMAND ----------

CREATE OR REPLACE TABLE movies
AS SELECT
  _c0 AS idx,
  title,
  CAST(year AS INT) AS year,
  CASE WHEN
    budget = 'NA' THEN 0
    ELSE CAST(budget AS INT)
  END AS budget,
  CAST(rating AS DOUBLE) AS rating
FROM hive_metastore.`${da.my_schema_name}`.movies

-- COMMAND ----------

-- DBTITLE 0,--i18n-b477ffe4-c5de-4fe7-8fec-559daf39e100
-- MAGIC %md
-- MAGIC If you are running queries as a secondary user, re-run the previous query in the Databricks SQL environment of the secondary user. Verify that:
-- MAGIC 1. the table can still be accessed
-- MAGIC 1. the table schema has been updated

-- COMMAND ----------

-- DBTITLE 0,--i18n-c1b1fada-41b8-41e8-af6f-2454369231db
-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC Run the following cell to remove the source database and table that was used in this example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
