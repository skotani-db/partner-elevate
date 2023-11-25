# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 識別できないPIIアクセス
# MAGIC
# MAGIC このレッスンでは、分析および報告のために潜在的にセンシティブな情報を扱う一方で、PII漏えいのリスクを低減するためのアプローチを探ります。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/ade/ADE_arch_user_bins.png" width="60%" />
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、受講者は以下のことができるようになります：
# MAGIC - 動的ビューを機密データに適用して、PII を含む列を見えなくする
# MAGIC - ダイナミックビューを使用してデータをフィルタリングし、関連する対象者に関連する行のみを表示する
# MAGIC - ビニングテーブルを作成してデータを一般化し、PII を見えなくする

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のセルを実行して、関連するデータベースとパスを設定することから始める。

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Views
# MAGIC Databricks の<a href="https://docs.databricks.com/security/access-control/table-acls/object-privileges.html#dynamic-view-functions" target="_blank">dynamic views</a>では、ユーザまたはグループの ID ACL をカラム（または行）レベルでデータに適用することができます。
# MAGIC
# MAGIC データベース管理者は、ソース・テーブルへのアクセスを禁止し、冗長化されたビューへのクエリのみをユーザに許可するように、データ・アクセス権限を構成することができます。
# MAGIC
# MAGIC 十分な権限を持つユーザはすべてのフィールドを見ることができますが、制限されたユーザにはビュー作成時に定義された任意の結果が表示されます。

# COMMAND ----------

# MAGIC %md
# MAGIC 以下のカラムを持つ **`users`** テーブルを考えてみよう。

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE users

# COMMAND ----------

# MAGIC %md
# MAGIC 姓、名、生年月日、住所は明らかに問題である。
# MAGIC
# MAGIC 郵便番号も読みにくくする（郵便番号と生年月日の組み合わせは、データを特定する可能性が非常に高いため）。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW users_vw AS
# MAGIC   SELECT
# MAGIC     alt_id,
# MAGIC     CASE 
# MAGIC       WHEN is_member('ade_demo') THEN dob
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS dob,
# MAGIC     sex,
# MAGIC     gender,
# MAGIC     CASE 
# MAGIC       WHEN is_member('ade_demo') THEN first_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS first_name,
# MAGIC     CASE 
# MAGIC       WHEN is_member('ade_demo') THEN last_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS last_name,
# MAGIC     CASE 
# MAGIC       WHEN is_member('ade_demo') THEN street_address
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS street_address,
# MAGIC     city,
# MAGIC     state,
# MAGIC     CASE 
# MAGIC       WHEN is_member('ade_demo') THEN zip
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS zip,
# MAGIC     updated
# MAGIC   FROM users

# COMMAND ----------

# MAGIC %md
# MAGIC これで、 **`users_vw`** からクエリを実行すると、 **`ade_demo`** グループのメンバーだけがプレーンテキストで結果を見ることができるようになります。
# MAGIC
# MAGIC **注意** あなたはグループを作成したり、メンバーを割り当てたりする権限を持っていないかもしれません。インストラクターは、グループメンバーシップによってクエリ結果がどのように変化するかを示すことができるはずです。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ## 条件付き行アクセスの追加
# MAGIC
# MAGIC ビューに **`WHERE`** 句を追加して、組織全体のチームに対してさまざまな条件でソースデータをフィルタリングすることは、各利用者に必要なデータのみへのアクセスを許可するための有益なオプションとなります。動的ビューは、昇格した権限を持つユーザーに対して、基礎データへの完全なアクセス権を持つこれらのビューを作成するオプションを追加します。
# MAGIC
# MAGIC 以下では、前のステップの **`users_vw`** を条件付きアクセスで変更しています。指定されたグループのメンバーでないユーザーは、指定された日付以降に更新されたロサンゼルス市のレコードのみを見ることができます。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW users_la_vw AS
# MAGIC SELECT * FROM users_vw
# MAGIC WHERE 
# MAGIC   CASE 
# MAGIC     WHEN is_member('ade_demo') THEN TRUE
# MAGIC     ELSE city = "Los Angeles" AND updated > "2019-12-12"
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users_la_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ## **`user_lookup`** テーブルへの仮アクセスの提供
# MAGIC
# MAGIC **`user_lookup`** テーブルは、ETLパイプラインが様々な識別子と **`alt_id`** を照合し、必要に応じて人口統計学的情報を引き出すことを可能にします。
# MAGIC
# MAGIC 私たちのチームのほとんどは、私たちの完全なPIIにアクセスする必要はありませんが、異なるシステムからの様々なナチュラルキーを照合するためにこのテーブルを使用する必要があるかもしれません。
# MAGIC
# MAGIC 以下に、 **`alt_id`** への条件付きアクセスを提供し、 **`user_lookup`** テーブルの他の情報へのフルアクセスを提供する、 **`user_lookup_vw`** というダイナミックビューを定義します。

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from user_lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC CREATE OR REPLACE VIEW user_lookup_vw AS
# MAGIC -- FILL_IN
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN is_member('ade_demo') THEN alt_id
# MAGIC     ELSE 'REDACTED'
# MAGIC   END AS alt_id,
# MAGIC   device_id,
# MAGIC   mac_address,
# MAGIC   user_id
# MAGIC FROM user_lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM user_lookup_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ## 集計テーブルでPIIを一般化する
# MAGIC
# MAGIC PIIの暴露の可能性を減らすもう1つのアプローチは、より特定的でないレベルでのデータへのアクセスのみを提供することです。
# MAGIC
# MAGIC このセクションでは、性別、都市、州の情報を維持したまま、ユーザーを年齢ビンに割り当てます。
# MAGIC
# MAGIC これによって、特定のユーザーの身元を明らかにすることなく、比較ダッシュボードを作成するのに十分な人口統計学的情報が提供されます。

# COMMAND ----------

# MAGIC %md
# MAGIC ここでは、値を手動で指定したラベルに置き換えるカスタムロジックを定義しているだけです。

# COMMAND ----------

def age_bins(dob_col):
    age_col = F.floor(F.months_between(F.current_date(), dob_col)/12).alias("age")
    
    return (F.when((age_col < 18), "under 18")
             .when((age_col >= 18) & (age_col < 25), "18-25")
             .when((age_col >= 25) & (age_col < 35), "25-35")
             .when((age_col >= 35) & (age_col < 45), "35-45")
             .when((age_col >= 45) & (age_col < 55), "45-55")
             .when((age_col >= 55) & (age_col < 65), "55-65")
             .when((age_col >= 65) & (age_col < 75), "65-75")
             .when((age_col >= 75) & (age_col < 85), "75-85")
             .when((age_col >= 85) & (age_col < 95), "85-95")
             .when((age_col >= 95), "95+")
             .otherwise("invalid age").alias("age"))

# COMMAND ----------

# MAGIC %md
# MAGIC この人口統計情報の集計ビューは、もはや個人を特定できるものではないので、自然キーを使って安全に保存することができる。
# MAGIC
# MAGIC IDを一致させるために、 **`user_lookup`** テーブルを参照します。

# COMMAND ----------

from pyspark.sql import functions as F

users_df = spark.table("users")
lookup_df = spark.table("user_lookup").select("alt_id", "user_id")

bins_df = users_df.join(lookup_df, ["alt_id"], "left").select("user_id", age_bins(F.col("dob")),"gender", "city", "state")

# COMMAND ----------

display(bins_df)

# COMMAND ----------

# MAGIC %md
# MAGIC このビニングされた人口統計データは、アナリストが参照できるようにテーブルに保存される。

# COMMAND ----------

(bins_df.write
        .format("delta")
        .option("path", f"{DA.paths.working_dir}/user_bins")
        .mode("overwrite")
        .saveAsTable("user_bins"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM user_bins

# COMMAND ----------

# MAGIC %md
# MAGIC 現在実装されているように、このロジックが処理されるたびに、すべてのレコードが新しく計算された値で上書きされることに注意してください。ビンの境界で生年月日を特定する可能性を減少させるために、年齢ビンを計算するために使用される値にランダムなノイズを追加することができます（一般的に年齢ビンは正確なままですが、正確な誕生日にユーザーを新しいビンに移行させる可能性を減少させます）。

# COMMAND ----------

# MAGIC %md 
# MAGIC 以下のセルを実行して、このレッスンに関連するテーブルとファイルを削除します。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
