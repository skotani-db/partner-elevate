-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-246366e0-139b-4ee6-8231-af9ffc8b9f20
-- MAGIC %md
-- MAGIC # Create and share tables in Unity Catalog
-- MAGIC
-- MAGIC In this notebook you will learn how to:
-- MAGIC * Create schemas and tables
-- MAGIC * Control access to schemas and tables
-- MAGIC * Explore grants on various objects in Unity Catalog

-- COMMAND ----------

-- DBTITLE 0,--i18n-675fb98e-c798-4468-9231-710a39216650
-- MAGIC %md
-- MAGIC ## Set Up
-- MAGIC
-- MAGIC Run the following cells to perform some setup. 
-- MAGIC
-- MAGIC In order to avoid conflicts in a shared training environment, this will generate a unique catalog name exclusively for your use. 
-- MAGIC
-- MAGIC In your own environment you are free to choose your own catalog names, but be careful about affecting other users & systems in that environment.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-06.2

-- COMMAND ----------

-- DBTITLE 0,--i18n-1f8f7a5b-7c09-4333-9057-f9e25f635f94
-- MAGIC %md
-- MAGIC ## Unity Catalog three-level namespace
-- MAGIC
-- MAGIC Most SQL developers will be familiar with using a two-level namespace to unambiguously address tables within a schema as follows:
-- MAGIC
-- MAGIC     SELECT * FROM schema.table;
-- MAGIC
-- MAGIC Unity Catalog introduces the concept of a *catalog* that resides above the schema in the object hierarchy. Metastores can host any number of catalogs, which in turn can host any number of schemas. To deal with this additional level, complete table references in Unity Catalog use a three-level namespace. The following statement exemplifies this:
-- MAGIC
-- MAGIC     SELECT * FROM catalog.schema.table;
-- MAGIC     
-- MAGIC SQL developers will probably also be familiar with the **`USE`** statement to select a default schema, to avoid having to always specify a schema when referencing tables. Unity Catalog augments this with the **`USE CATALOG`** statement, which similarly selects a default catalog.
-- MAGIC
-- MAGIC To simplify your experience, we ensured the catalog was created and set it as the default as you can see in the following command.

-- COMMAND ----------

SELECT current_catalog(), current_database()

-- COMMAND ----------

-- DBTITLE 0,--i18n-7608a78c-6f96-4f0b-a9fb-b303c76ad899
-- MAGIC %md
-- MAGIC ## Create and use a new schema
-- MAGIC
-- MAGIC Let's create a new schema exclusively for our use in this exercise, then set this as the default so we can reference tables by name only.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS my_own_schema;
USE my_own_schema;

SELECT current_database()

-- COMMAND ----------

-- DBTITLE 0,--i18n-013687c8-6a3f-4c8b-81c1-0afb0429914f
-- MAGIC %md
-- MAGIC ## Create Delta architecture
-- MAGIC
-- MAGIC Let's create and populate a simple collection of schemas and tables persuant to the Delta architecture:
-- MAGIC * A silver schema containing patient heart rate data as read from a medical device
-- MAGIC * A gold schema table that averages heart rate data per patient on a daily basis
-- MAGIC
-- MAGIC For now, there will be no bronze table in this simple example.
-- MAGIC
-- MAGIC Note that we need ony specify the table name below, since we have set a default catalog and schema above.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS patient_silver;

CREATE OR REPLACE TABLE patient_silver.heartrate (
  device_id  INT,
  mrn        STRING,
  name       STRING,
  time       TIMESTAMP,
  heartrate  DOUBLE
);

INSERT INTO patient_silver.heartrate VALUES
  (23,'40580129','Nicholas Spears','2020-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177','Lynn Russell','2020-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842','Samuel Hughes','2020-02-01T00:08:58.000+0000',52.1354807863),
  (23,'40580129','Nicholas Spears','2020-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',95.033344842),
  (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',57.3391541312),
  (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',56.6165053697),
  (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',94.8134313932),
  (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',56.2469995332),
  (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',54.8372685558)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS patient_gold;

CREATE OR REPLACE TABLE patient_gold.heartrate_stats AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM patient_silver.heartrate
  GROUP BY mrn, name, DATE_TRUNC("DD", time)
);
  
SELECT * FROM patient_gold.heartrate_stats;  

-- COMMAND ----------

-- DBTITLE 0,--i18n-3961e85e-2bba-460a-9e00-12e37a07cb87
-- MAGIC %md
-- MAGIC ## Grant access to gold schema [optional]
-- MAGIC
-- MAGIC Now let's allow users in the **account users** group to read from the **gold** schema.
-- MAGIC
-- MAGIC Perform this section by uncommenting the code cells and running them in sequence. 
-- MAGIC You will also be prompted to run some queries. 
-- MAGIC
-- MAGIC To do this:
-- MAGIC 1. Open a separate browser tab and load your Databricks workspace.
-- MAGIC 1. Switch to Databricks SQL by clicking on the app switcher and selecting SQL.
-- MAGIC 1. Create a SQL warehouse following the instructions in *Create SQL Warehouse in Unity Catalog*.
-- MAGIC 1. Prepare to enter queries as instructed below in that environment.

-- COMMAND ----------

-- DBTITLE 0,--i18n-e4ec17fc-7dfa-4d3f-a82f-02eca61e6e53
-- MAGIC %md
-- MAGIC Let's grant **SELECT** privilege on the **gold** table.

-- COMMAND ----------

-- GRANT SELECT ON TABLE patient_gold.heartrate_stats to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-d20d5b94-b672-48cf-865a-7c9aa0629955
-- MAGIC %md
-- MAGIC ### Query table as user
-- MAGIC
-- MAGIC With a **SELECT** grant in place, attempt to query the table in the Databricks SQL environment.
-- MAGIC
-- MAGIC Run the following cell to output a query statement that reads from the **gold** table. Copy and paste the output into a new query within the SQL environment and run the query.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog_name}.patient_gold.heartrate_stats")

-- COMMAND ----------

-- DBTITLE 0,--i18n-3309187d-5a52-4b51-9198-7fc54ea9ca84
-- MAGIC %md
-- MAGIC This will not work yet, because **SELECT** privilege on the table alone is insufficient. **USAGE** privilege is also required on the containing elements. Let's correct this now by executing the following.

-- COMMAND ----------

-- GRANT USAGE ON CATALOG ${DA.catalog_name} TO `account users`;
-- GRANT USAGE ON SCHEMA patient_gold TO `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-a6fcc71b-ceb3-4067-acb2-85504ad4d6ea
-- MAGIC %md
-- MAGIC
-- MAGIC Repeat the query in the Databricks SQL environment, and with these two grants in place the operation should succeed.

-- COMMAND ----------

-- DBTITLE 0,--i18n-5dd6b7c1-6fed-4d09-b808-0615a10b2502
-- MAGIC %md
-- MAGIC
-- MAGIC ## Explore grants
-- MAGIC
-- MAGIC Let's explore the grants on some of the objects in the Unity Catalog hierarchy, starting with the **gold** table.

-- COMMAND ----------

-- SHOW GRANT ON TABLE ${DA.catalog_name}.patient_gold.heartrate_stats

-- COMMAND ----------

-- DBTITLE 0,--i18n-e7b6ddda-b6ef-4558-a5b6-c9f97bb7db80
-- MAGIC %md
-- MAGIC Currently there is only the **SELECT** grant we set up earlier. Now let's check the grants on **silver**.

-- COMMAND ----------

SHOW TABLES IN ${DA.catalog_name}.patient_silver;

-- COMMAND ----------

-- SHOW GRANT ON TABLE ${DA.catalog_name}.patient_silver.heartrate

-- COMMAND ----------

-- DBTITLE 0,--i18n-1e9d3c88-af92-4755-a01c-6e0aefdd8c9d
-- MAGIC %md
-- MAGIC There are currently no grants on this table; only the owner can access this table.
-- MAGIC
-- MAGIC Now let's look at the containing schema.

-- COMMAND ----------

-- SHOW GRANT ON SCHEMA ${DA.catalog_name}.patient_silver

-- COMMAND ----------

-- DBTITLE 0,--i18n-17125954-7b25-40dc-ab84-5a159198e9fc
-- MAGIC %md
-- MAGIC There are currently no grants on this schema. 
-- MAGIC
-- MAGIC Now let's examine the catalog.

-- COMMAND ----------

-- SHOW GRANT ON CATALOG `${DA.catalog_name}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-7453efa0-62d0-4af2-901f-c222dd9b2d07
-- MAGIC %md
-- MAGIC Currently we see the **USAGE** grant we set up earlier.

-- COMMAND ----------

-- DBTITLE 0,--i18n-b4f88042-e7cb-4a1f-b853-cb328356778c
-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC Run the following cell to remove the schema that we created in this example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
