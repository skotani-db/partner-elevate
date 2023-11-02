# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup-05.2.4L

# COMMAND ----------

# DBTITLE 0,--i18n-2b2de306-5587-4e60-aa3f-34ae6c344907
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Exploring the Results of a DLT Pipeline
# MAGIC
# MAGIC Run the following cell to enumerate the output of your storage location:

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-f8f98f46-f3d3-41e5-b86a-fc236813e67e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC The **system** directory captures events associated with the pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-b8afa35d-667e-40b8-915c-d754d5bdb5ee
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC These event logs are stored as a Delta table. 
# MAGIC
# MAGIC Let's query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.storage_location}/system/events`

# COMMAND ----------

# DBTITLE 0,--i18n-adc883c4-30ec-4391-8d7f-9554f77a0feb
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Let's view the contents of the *tables* directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-72eb29d2-cde0-4488-954b-a0ed47ead8eb
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Let's query the gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${DA.schema_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()

