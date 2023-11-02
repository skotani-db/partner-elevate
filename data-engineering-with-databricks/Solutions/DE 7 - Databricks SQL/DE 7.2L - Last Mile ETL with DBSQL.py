# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-9390aad6-4d8b-4e7f-8851-18ca0b6bf7c6
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Last Mile ETL with Databricks SQL
# MAGIC
# MAGIC Before we continue, let's do a recap of some of the things we've learned so far:
# MAGIC 1. The Databricks workspace contains a suite of tools to simplify the data engineering development lifecycle
# MAGIC 1. Databricks notebooks allow users to mix SQL with other programming languages to define ETL workloads
# MAGIC 1. Delta Lake provides ACID compliant transactions and makes incremental data processing easy in the Lakehouse
# MAGIC 1. Delta Live Tables extends the SQL syntax to support many design patterns in the Lakehouse, and simplifies infrastructure deployment
# MAGIC 1. Multi-task jobs allows for full task orchestration, adding dependencies while scheduling a mix of notebooks and DLT pipelines
# MAGIC 1. Databricks SQL allows users to edit and execute SQL queries, build visualizations, and define dashboards
# MAGIC 1. Data Explorer simplifies managing Table ACLs, making Lakehouse data available to SQL analysts (soon to be expanded greatly by Unity Catalog)
# MAGIC
# MAGIC In this section, we'll focus on exploring more DBSQL functionality to support production workloads. 
# MAGIC
# MAGIC We'll start by focusing on leveraging Databricks SQL to configure queries that support last mile ETL for analytics. Note that while we'll be using the Databricks SQL UI for this demo, SQL Warehouses <a href="https://docs.databricks.com/integrations/partners.html" target="_blank">integrate with a number of other tools to allow external query execution</a>, as well as having <a href="https://docs.databricks.com/sql/api/index.html" target="_blank">full API support for executing arbitrary queries programmatically</a>.
# MAGIC
# MAGIC From these query results, we'll generate a series of visualizations, which we'll combine into a dashboard.
# MAGIC
# MAGIC Finally, we'll walk through scheduling updates for queries and dashboards, and demonstrate setting alerts to help monitor the state of production datasets over time.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Use Databricks SQL as a tool to support production ETL tasks backing analytic workloads
# MAGIC * Configure SQL queries and visualizations with the Databricks SQL Editor
# MAGIC * Create dashboards in Databricks SQL
# MAGIC * Schedule updates for queries and dashboards
# MAGIC * Set alerts for SQL queries

# COMMAND ----------

# DBTITLE 0,--i18n-9ff94613-d58b-48fe-b32d-3718cbcb2f30
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Run Setup Script
# MAGIC The following cells runs a notebook that defines a class we'll use to generate SQL queries.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-07.2

# COMMAND ----------

# DBTITLE 0,--i18n-c384931f-c934-440f-9f0c-a7b04c1c28b0
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Create a Demo Database
# MAGIC Execute the following cell and copy the results into the Databricks SQL Editor.
# MAGIC
# MAGIC These queries:
# MAGIC * Create a new database
# MAGIC * Declare two tables (we'll use these for loading data)
# MAGIC * Declare two functions (we'll use these for generating data)
# MAGIC
# MAGIC Once copied, execute the query using the **Run** button.

# COMMAND ----------

DA.generate_config()

# COMMAND ----------

# DBTITLE 0,--i18n-9cd7f410-eead-4759-bd99-83eafb03d0df
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC **NOTE**: The queries above are only designed to be run once after resetting the demo completely to reconfigure the environment. 
# MAGIC
# MAGIC Users will need to have **`CREATE`** and **`USAGE`** permissions on the catalog to execute them.

# COMMAND ----------

# DBTITLE 0,--i18n-3652d1e3-fc73-44f9-9b07-6b1b8b5343d6
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> 
# MAGIC **WARNING:** Make sure to select your database before proceeding as the **`USE`** statement<br/>doesn't yet change the database against which your queries will execute

# COMMAND ----------

# DBTITLE 0,--i18n-a0b2608d-d22e-4085-b02f-be43b744ecf2
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Create a Query to Load Data
# MAGIC Steps:
# MAGIC 1. Execute the cell below to print out a formatted SQL query for loading data in the **`user_ping`** table created in the previous step.
# MAGIC 1. Save this query with the name **Load Ping Data**.
# MAGIC 1. Run this query to load a batch of data.

# COMMAND ----------

DA.generate_load()

# COMMAND ----------

# DBTITLE 0,--i18n-48fc3085-b405-450f-9463-ee1501cd56aa
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Executing the query should load some data and return a preview of the data in the table.
# MAGIC
# MAGIC **NOTE**: Random numbers are being used to define and load data, so each user will have slightly different values present.

# COMMAND ----------

# DBTITLE 0,--i18n-7dc1c1c9-96c5-49f7-a1ab-deb44594cddf
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Set a Query Refresh Schedule
# MAGIC
# MAGIC Steps:
# MAGIC 1. Locate the **Refresh Schedule** field at the bottom right of the SQL query editor box; click the blue **Never**
# MAGIC 1. Use the drop down to change to Refresh every **1 week** at **12:00**
# MAGIC 1. For **Ends**, click the **On** radio button
# MAGIC 1. Select tomorrow's date
# MAGIC 1. Click **OK**
# MAGIC
# MAGIC **NOTE:** Although we are using a refresh schedule of 1 week for classroom purposes, you'll likely see shorter trigger intervals in production, such as schedules to refresh every 1 minute.

# COMMAND ----------

# DBTITLE 0,--i18n-58aa7a7b-4b43-429d-8603-0cc1001a5ae6
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Create a Query to Track Total Records
# MAGIC Steps:
# MAGIC 1. Execute the cell below.
# MAGIC 1. Save this query with the name **User Counts**.
# MAGIC 1. Run the query to calculate the current results.

# COMMAND ----------

DA.generate_user_counts()

# COMMAND ----------

# DBTITLE 0,--i18n-c43c8f47-c8c3-4232-9a76-f82bdd204317
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Create a Bar Graph Visualization
# MAGIC
# MAGIC Steps:
# MAGIC 1. Click the **+** sign next to the results tab in middle of window, and select **Visualization** from the dialog box
# MAGIC 1. Click on the name (should default to something like **`Bar 1`**) and change the name to **Total User Records**
# MAGIC 1. Set **`user_id`** for the **X Column**
# MAGIC 1. Set **`total_records`** for the **Y Column**
# MAGIC 1. Click **Save**

# COMMAND ----------

# DBTITLE 0,--i18n-644501a5-c5fc-45e0-bd34-728413b5d267
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Create a New Dashboard
# MAGIC
# MAGIC Steps:
# MAGIC 1. Click the button with three vertical dots at the bottom of the screen and select **Add to Dashboard**.
# MAGIC 1. Click the **Create new dashboard** option
# MAGIC 1. Name your dashboard <strong>User Ping Summary **`<your_initials_here>`**</strong>
# MAGIC 1. Click **Save** to create the new dashboard
# MAGIC 1. Your newly created dashboard should now be selected as the target; click **OK** to add your visualization

# COMMAND ----------

# DBTITLE 0,--i18n-400bfcee-6c58-4863-b876-609950543f6f
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Create a Query to Calculate the Recent Average Ping
# MAGIC Steps:
# MAGIC 1. Execute the cell below to print out the formatted SQL query.
# MAGIC 1. Save this query with the name **Avg Ping**.
# MAGIC 1. Run the query to calculate the current results.

# COMMAND ----------

DA.generate_avg_ping()

# COMMAND ----------

# DBTITLE 0,--i18n-82ba8c89-8e4b-4ab5-817a-1161017a1168
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Add a Line Plot Visualization to your Dashboard
# MAGIC
# MAGIC Steps:
# MAGIC 1. Click the **Add Visualization** button
# MAGIC 1. Click on the name (should default to something like **`Visualization 1`**) and change the name to **Avg User Ping**
# MAGIC 1. Select **`Line`** for the **Visualization Type**
# MAGIC 1. Set **`end_time`** for the **X Column**
# MAGIC 1. Set **`avg_ping`** for the **Y Columns**
# MAGIC 1. Set **`user_id`** for the **Group by**
# MAGIC 1. Click **Save**
# MAGIC 1. Click the button with three vertical dots at the bottom of the screen and select **Add to Dashboard**.
# MAGIC 1. Select the dashboard you created earlier
# MAGIC 1. Click **OK** to add your visualization

# COMMAND ----------

# DBTITLE 0,--i18n-9088bbce-cf24-4731-80c0-15972786eda1
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Create a Query to Report Summary Statistics
# MAGIC Steps:
# MAGIC 1. Execute the cell below.
# MAGIC 1. Save this query with the name **Ping Summary**.
# MAGIC 1. Run the query to calculate the current results.

# COMMAND ----------

DA.generate_summary()

# COMMAND ----------

# DBTITLE 0,--i18n-e5832d13-b8f3-40db-95cb-8c24e895cda7
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Add the Summary Table to your Dashboard
# MAGIC
# MAGIC Steps:
# MAGIC 1. Click the button with three vertical dots at the bottom of the screen and select **Add to Dashboard**.
# MAGIC 1. Select the dashboard you created earlier
# MAGIC 1. Click **OK** to add your visualization

# COMMAND ----------

# DBTITLE 0,--i18n-626f8b1d-51cd-47b0-8828-b35180acb40c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Review and Refresh your Dashboard
# MAGIC
# MAGIC Steps:
# MAGIC 1. Use the left side bar to navigate to **Dashboards**
# MAGIC 1. Find the dashboard you've added your queries to
# MAGIC 1. Click the blue **Refresh** button to update your dashboard
# MAGIC 1. Click the **Schedule** button to review dashboard scheduling options
# MAGIC   * Note that scheduling a dashboard to update will execute all queries associated with that dashboard
# MAGIC   * Do not schedule the dashboard at this time

# COMMAND ----------

# DBTITLE 0,--i18n-4f69c53f-8bd9-48d5-9f6f-f97045581e49
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Share your Dashboard
# MAGIC
# MAGIC Steps:
# MAGIC 1. Click the blue **Share** button
# MAGIC 1. Select **All Users** from the top field
# MAGIC 1. Choose **Can Run** from the right field
# MAGIC 1. Click **Add**
# MAGIC 1. Change the **Credentials** to **Run as viewer**
# MAGIC
# MAGIC **NOTE**: At present, no other users should have any permissions to run your dashboard, as they have not been granted permissions to the underlying databases and tables using Table ACLs. If you wish other users to be able to trigger updates to your dashboard, you will either need to grant them permissions to **Run as owner** or add permissions for the tables referenced in your queries.

# COMMAND ----------

# DBTITLE 0,--i18n-facded12-10b1-4c63-a075-b7790fa3cd17
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Set Up an Alert
# MAGIC
# MAGIC Steps:
# MAGIC 1. Use the left side bar to navigate to **Alerts**
# MAGIC 1. Click **Create Alert** in the top right
# MAGIC 1. Select your **User Counts** query
# MAGIC 1. Click the field at the top left of the screen to give the alert a name **`<your_initials> Count Check`**
# MAGIC 1. For the **Trigger when** options, configure:
# MAGIC   * **Value column**: **`total_records`**
# MAGIC   * **Condition**: **`>`**
# MAGIC   * **Threshold**: **`15`**
# MAGIC 1. For **Refresh**, select **Never**
# MAGIC 1. Click **Create Alert**
# MAGIC 1. On the next screen, click the blue **Refresh** in the top right to evaluate the alert

# COMMAND ----------

# DBTITLE 0,--i18n-f9f22ebd-4283-474a-ab45-0d7584a6b6ac
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Review Alert Destination Options
# MAGIC
# MAGIC
# MAGIC
# MAGIC Steps:
# MAGIC 1. From the preview of your alert, click the blue **Add** button to the right of **Destinations** on the right side of the screen
# MAGIC 1. At the bottom of the window that pops up, locate the and click the blue text in the message **Create new destinations in Alert Destinations**
# MAGIC 1. Review the available alerting options

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
