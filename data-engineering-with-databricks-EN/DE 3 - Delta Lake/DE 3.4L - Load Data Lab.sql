-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-02080b66-d11c-47fb-a096-38ce02af4dbb
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC # Load Data Lab
-- MAGIC
-- MAGIC In this lab, you will load data into new and existing Delta tables.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Create an empty Delta table with a provided schema
-- MAGIC - Insert records from an existing table into a Delta table
-- MAGIC - Use a CTAS statement to create a Delta table from files

-- COMMAND ----------

-- DBTITLE 0,--i18n-50357195-09c5-4ab4-9d60-ca7fd44feecc
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Run Setup
-- MAGIC
-- MAGIC Run the following cell to configure variables and datasets for this lesson.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.6L

-- COMMAND ----------

-- DBTITLE 0,--i18n-5b20c9b2-6658-4536-b79b-171d984b3b1e
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Data Overview
-- MAGIC
-- MAGIC We will work with a sample of raw Kafka data written as JSON files. 
-- MAGIC
-- MAGIC Each file contains all records consumed during a 5-second interval, stored with the full Kafka schema as a multiple-record JSON file. 
-- MAGIC
-- MAGIC The schema for the table:
-- MAGIC
-- MAGIC | field  | type | description |
-- MAGIC | ------ | ---- | ----------- |
-- MAGIC | key    | BINARY | The **`user_id`** field is used as the key; this is a unique alphanumeric field that corresponds to session/cookie information |
-- MAGIC | offset | LONG | This is a unique value, monotonically increasing for each partition |
-- MAGIC | partition | INTEGER | Our current Kafka implementation uses only 2 partitions (0 and 1) |
-- MAGIC | timestamp | LONG    | This timestamp is recorded as milliseconds since epoch, and represents the time at which the producer appends a record to a partition |
-- MAGIC | topic | STRING | While the Kafka service hosts multiple topics, only those records from the **`clickstream`** topic are included here |
-- MAGIC | value | BINARY | This is the full data payload (to be discussed later), sent as JSON |

-- COMMAND ----------

-- DBTITLE 0,--i18n-5140f012-898a-43ac-bed9-b7e01a916505
-- MAGIC %md
-- MAGIC
-- MAGIC ## Define Schema for Empty Delta Table
-- MAGIC Create an empty managed Delta table named **`events_raw`** using the same schema.

-- COMMAND ----------

-- TODO
<FILL_IN>

-- COMMAND ----------

-- DBTITLE 0,--i18n-70f4dbf1-f4cb-4ec6-925a-a939cbc71bd5
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Run the cell below to confirm the table was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Define Schema")
-- MAGIC expected_table = lambda: spark.table("events_raw")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"events_raw\"")
-- MAGIC suite.test_equals(lambda: expected_table().count(), 0, "The table should have 0 records")
-- MAGIC
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "key", "BinaryType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "offset", "LongType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "partition", "IntegerType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "timestamp", "LongType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "topic", "StringType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "value", "BinaryType")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite

-- COMMAND ----------

-- DBTITLE 0,--i18n-7b4e55f2-737f-4996-a51b-4f18c2fc6eb7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Insert Raw Events Into Delta Table
-- MAGIC
-- MAGIC Once the extracted data and Delta table are ready, insert the JSON records from the **`events_json`** table into the new **`events_raw`** Delta table.

-- COMMAND ----------

-- TODO
<FILL_IN>

-- COMMAND ----------

-- DBTITLE 0,--i18n-65ffce96-d821-4792-b545-5725814003a0
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Manually review the table contents to ensure data was written as expected.

-- COMMAND ----------

-- TODO
<FILL_IN>

-- COMMAND ----------

-- DBTITLE 0,--i18n-1cbff40d-e916-487c-958f-2eb7807c5aeb
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Run the cell below to confirm the data has been loaded correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Validate events_raw")
-- MAGIC expected_table = lambda: spark.table("events_raw")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"events_raw\"")
-- MAGIC suite.test_equals(lambda: expected_table().count(), 2252, "The table should have 2252 records")
-- MAGIC
-- MAGIC first_five = lambda: [r["timestamp"] for r in expected_table().orderBy(F.col("timestamp").asc()).limit(5).collect()]
-- MAGIC suite.test_sequence(first_five, [1593879303631, 1593879304224, 1593879305465, 1593879305482, 1593879305746], True, "First 5 values are correct")
-- MAGIC
-- MAGIC last_five = lambda: [r["timestamp"] for r in expected_table().orderBy(F.col("timestamp").desc()).limit(5).collect()]
-- MAGIC suite.test_sequence(last_five, [1593881096290, 1593881095799, 1593881093452, 1593881093394, 1593881092076], True, "Last 5 values are correct")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite.passed

-- COMMAND ----------

-- DBTITLE 0,--i18n-b3c62fea-b75d-41d6-8214-660f9cfa3acd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Create a Delta Table From Query Results
-- MAGIC
-- MAGIC In addition to new events data, let's also load a small lookup table that provides product details that we'll use later in the course.
-- MAGIC Use a CTAS statement to create a managed Delta table named **`item_lookup`** that extracts data from the parquet directory provided below.

-- COMMAND ----------

-- TODO
<FILL_IN> ${da.paths.datasets}/ecommerce/raw/item-lookup

-- COMMAND ----------

-- DBTITLE 0,--i18n-5a971532-0003-4665-9064-26196cd31e89
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Run the cell below to confirm the lookup table has been loaded correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Validate item_lookup")
-- MAGIC expected_table = lambda: spark.table("item_lookup")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"item_lookup\"")
-- MAGIC
-- MAGIC actual_values = lambda: [r["item_id"] for r in expected_table().collect()]
-- MAGIC expected_values = ['M_PREM_Q','M_STAN_F','M_PREM_F','M_PREM_T','M_PREM_K','P_DOWN_S','M_STAN_Q','M_STAN_K','M_STAN_T','P_FOAM_S','P_FOAM_K','P_DOWN_K']
-- MAGIC suite.test_sequence(actual_values, expected_values, False, "Contains the 12 expected item IDs")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite.passed

-- COMMAND ----------

-- DBTITLE 0,--i18n-4db73493-3920-44e2-a19b-f335aa650f76
-- MAGIC %md
-- MAGIC
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
