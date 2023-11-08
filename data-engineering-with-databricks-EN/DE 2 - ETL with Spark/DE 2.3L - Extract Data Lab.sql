-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-037a5204-a995-4945-b6ed-7207b636818c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC # Extract Data Lab
-- MAGIC
-- MAGIC In this lab, you will extract raw data from JSON files.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Register an external table to extract data from JSON files

-- COMMAND ----------

-- DBTITLE 0,--i18n-3b401203-34ae-4de5-a706-0dbbd5c987b7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Run Setup
-- MAGIC
-- MAGIC Run the following cell to configure variables and datasets for this lesson.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.3L

-- COMMAND ----------

-- DBTITLE 0,--i18n-9adc06e2-7298-4306-a062-7ff70adb8032
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Overview of the Data
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

-- DBTITLE 0,--i18n-8cca978a-3034-4339-8e6e-6c48d389cce7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC  
-- MAGIC ## Extract Raw Events From JSON Files
-- MAGIC To load this data into Delta properly, we first need to extract the JSON data using the correct schema.
-- MAGIC
-- MAGIC Create an external table against JSON files located at the filepath provided below. Name this table **`events_json`** and declare the schema above.

-- COMMAND ----------

-- TODO
<FILL_IN> "${DA.paths.kafka_events}" 

-- COMMAND ----------

-- DBTITLE 0,--i18n-33231985-3ff1-4f44-8098-b7b862117689
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC **NOTE**: We'll use Python to run checks occasionally throughout the lab. The following cell will return an error with a message on what needs to change if you have not followed instructions. No output from cell execution means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_json"), "Table named `events_json` does not exist"
-- MAGIC assert spark.table("events_json").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_json").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC
-- MAGIC total = spark.table("events_json").count()
-- MAGIC assert total == 2252, f"Expected 2252 records, found {total}"

-- COMMAND ----------

-- DBTITLE 0,--i18n-6919d58a-89e4-4c02-812c-98a15bb6f239
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
