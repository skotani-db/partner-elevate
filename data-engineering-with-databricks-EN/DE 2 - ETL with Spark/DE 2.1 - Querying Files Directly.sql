-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-a0d28fb8-0d0f-4354-9720-79ce468b5ea8
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Extracting Data Directly From Files with Spark SQL
-- MAGIC
-- MAGIC In this notebook, you'll learn to extract data directly from files using Spark SQL on Databricks.
-- MAGIC
-- MAGIC A number of file formats support this option, but it is most useful for self-describing data formats (such as Parquet and JSON).
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Use Spark SQL to directly query data files
-- MAGIC - Layer views and CTEs to make referencing data files easier
-- MAGIC - Leverage **`text`** and **`binaryFile`** methods to review raw file contents

-- COMMAND ----------

-- DBTITLE 0,--i18n-73162404-8907-47f6-9b3e-dd17819d71c9
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Run Setup
-- MAGIC
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.1

-- COMMAND ----------

-- DBTITLE 0,--i18n-480bfe0b-d36d-4f67-8242-6a6d3cca38dd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Data Overview
-- MAGIC
-- MAGIC In this example, we'll work with a sample of raw Kafka data written as JSON files. 
-- MAGIC
-- MAGIC Each file contains all records consumed during a 5-second interval, stored with the full Kafka schema as a multiple-record JSON file.
-- MAGIC
-- MAGIC | field | type | description |
-- MAGIC | --- | --- | --- |
-- MAGIC | key | BINARY | The **`user_id`** field is used as the key; this is a unique alphanumeric field that corresponds to session/cookie information |
-- MAGIC | value | BINARY | This is the full data payload (to be discussed later), sent as JSON |
-- MAGIC | topic | STRING | While the Kafka service hosts multiple topics, only those records from the **`clickstream`** topic are included here |
-- MAGIC | partition | INTEGER | Our current Kafka implementation uses only 2 partitions (0 and 1) |
-- MAGIC | offset | LONG | This is a unique value, monotonically increasing for each partition |
-- MAGIC | timestamp | LONG | This timestamp is recorded as milliseconds since epoch, and represents the time at which the producer appends a record to a partition |

-- COMMAND ----------

-- DBTITLE 0,--i18n-65941466-ca87-4c29-903e-658e24e48cee
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note that our source directory contains many JSON files.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(DA.paths.kafka_events)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(DA.paths.kafka_events)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-f1ddfb40-9c95-4b9a-84e5-2958ac01166d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Here, we'll be using relative file paths to data that's been written to the DBFS root. 
-- MAGIC
-- MAGIC Most workflows will require users to access data from external cloud storage locations. 
-- MAGIC
-- MAGIC In most companies, a workspace administrator will be responsible for configuring access to these storage locations.
-- MAGIC
-- MAGIC Instructions for configuring and accessing these locations can be found in the cloud-vendor specific self-paced courses titled "Cloud Architecture & Systems Integrations".

-- COMMAND ----------

-- DBTITLE 0,--i18n-9abfecfc-df3f-4697-8880-bd3f0b58a864
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Query a Single File
-- MAGIC
-- MAGIC To query the data contained in a single file, execute the query with the following pattern:
-- MAGIC
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>
-- MAGIC
-- MAGIC Make special note of the use of back-ticks (not single quotes) around the path.

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c2891f1-e055-4fde-8bf9-3f448e4cdb2b
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note that our preview displays all 321 rows of our source file.

-- COMMAND ----------

-- DBTITLE 0,--i18n-0f45ecb7-4024-4798-a9b8-e46ac939b2f7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Query a Directory of Files
-- MAGIC
-- MAGIC Assuming all of the files in a directory have the same format and schema, all files can be queried simultaneously by specifying the directory path rather than an individual file.

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-6921da25-dc10-4bd9-9baa-7e589acd3139
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC By default, this query will only show the first 1000 rows.

-- COMMAND ----------

-- DBTITLE 0,--i18n-035ddfa2-76af-4e5e-a387-71f26f8c7f76
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Create References to Files
-- MAGIC This ability to directly query files and directories means that additional Spark logic can be chained to queries against files.
-- MAGIC
-- MAGIC When we create a view from a query against a path, we can reference this view in later queries.

-- COMMAND ----------

CREATE OR REPLACE VIEW event_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c29b73b-b4b0-48ab-afbb-7b1422fce6e4
-- MAGIC %md
-- MAGIC
-- MAGIC As long as a user has permission to access the view and the underlying storage location, that user will be able to use this view definition to query the underlying data. This applies to different users in the workspace, different notebooks, and different clusters.

-- COMMAND ----------

SELECT * FROM event_view

-- COMMAND ----------

-- DBTITLE 0,--i18n-efd0c0fc-5346-4275-b083-4ee96ce8a852
-- MAGIC %md
-- MAGIC ## Create Temporary References to Files
-- MAGIC
-- MAGIC Temporary views similarly alias queries to a name that's easier to reference in later queries.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-a9f9827b-2258-4481-a9d9-6fecf55aeb9b
-- MAGIC %md
-- MAGIC
-- MAGIC Temporary views exists only for the current SparkSession. On Databricks, this means they are isolated to the current notebook, job, or DBSQL query.

-- COMMAND ----------

SELECT * FROM events_temp_view

-- COMMAND ----------

-- DBTITLE 0,--i18n-dcfaeef2-0c3b-4782-90a6-5e0332dba614
-- MAGIC %md
-- MAGIC ## Apply CTEs for Reference within a Query 
-- MAGIC Common table expressions (CTEs) are perfect when you want a short-lived, human-readable reference to the results of a query.

-- COMMAND ----------

WITH cte_json
AS (SELECT * FROM json.`${DA.paths.kafka_events}`)
SELECT * FROM cte_json

-- COMMAND ----------

-- DBTITLE 0,--i18n-c85e1553-f643-47b8-b909-0d10d2177437
-- MAGIC %md
-- MAGIC CTEs only alias the results of a query while that query is being planned and executed.
-- MAGIC
-- MAGIC As such, **the following cell with throw an error when executed**.

-- COMMAND ----------

-- SELECT COUNT(*) FROM cte_json

-- COMMAND ----------

-- DBTITLE 0,--i18n-106214eb-2fec-4a27-b692-035a86b8ec8d
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Extract Text Files as Raw Strings
-- MAGIC
-- MAGIC When working with text-based files (which include JSON, CSV, TSV, and TXT formats), you can use the **`text`** format to load each line of the file as a row with one string column named **`value`**. This can be useful when data sources are prone to corruption and custom text parsing functions will be used to extract values from text fields.

-- COMMAND ----------

SELECT * FROM text.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-732e648b-4274-48f4-86e9-8b42fd5a26bd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Extract the Raw Bytes and Metadata of a File
-- MAGIC
-- MAGIC Some workflows may require working with entire files, such as when dealing with images or unstructured data. Using **`binaryFile`** to query a directory will provide file metadata alongside the binary representation of the file contents.
-- MAGIC
-- MAGIC Specifically, the fields created will indicate the **`path`**, **`modificationTime`**, **`length`**, and **`content`**.

-- COMMAND ----------

SELECT * FROM binaryFile.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-9ac20d39-ae6a-400e-9e13-14af5d4c91df
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
