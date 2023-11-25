# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # ストリーミングジョブデモのまとめ
# MAGIC
# MAGIC 主なポイント
# MAGIC * 高利用時には、複数のストリームを1つのタスクに組み合わせることができます。
# MAGIC * 高度に重要なストリームの場合、それらを単一のタスクに分離することができます。
# MAGIC * 常時オンのストリームと実行時ストリーム、利用可能時ストリームの違い
# MAGIC * タスク固有のクラスタと汎用クラスタの違い
# MAGIC * ウォームプールの威力
# MAGIC
# MAGIC 以上が、ストリーミングジョブデモのまとめです。

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.4.4

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC * ジョブを停止/一時停止します。
# MAGIC * 以下のセルを実行して、このレッスンに関連するテーブルやファイルを削除します。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
