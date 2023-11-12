# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-17f3cfcf-2ad7-48f2-bf4e-c2adcb372926
# MAGIC %md
# MAGIC # DLT Pythonの構文のトラブルシューティング
# MAGIC
# MAGIC 2つのノートブックを構成して実行するプロセスを実行したので、3番目のノートブックを開発して追加するプロセスをシミュレーションします。
# MAGIC
# MAGIC **心配しないでください！**
# MAGIC
# MAGIC 以下で提供されるコードには、意図的な小さな構文エラーがいくつか含まれています。これらのエラーをトラブルシューティングすることで、DLTコードを段階的に開発し、構文のエラーを特定する方法を学びます。
# MAGIC
# MAGIC このレッスンは、コード開発とテストのための堅牢なソリューションを提供するものではありません。代わりに、DLTを始めて使っており、なじみのない構文に苦労しているユーザーに役立つことを意図しています。
# MAGIC
# MAGIC ## 学習目標
# MAGIC このレッスンの終わりまでに、学生は次のことを快適に感じるはずです:
# MAGIC * DLTの構文を特定し、トラブルシューティングする方法
# MAGIC * ノートブックを使用してDLTパイプラインを段階的に開発する方法

# COMMAND ----------

# DBTITLE 0,--i18n-b6eb7861-af09-4009-a272-1c5c91f87a8b
# MAGIC %md
# MAGIC ## このノートブックをDLTパイプラインに追加
# MAGIC
# MAGIC このコースのこの段階では、2つのノートブックライブラリで構成されたDLTパイプラインを構成する必要があります。
# MAGIC
# MAGIC このパイプラインを通じて複数のレコードのバッチを処理し、パイプラインを新たに実行して追加のライブラリを追加する方法を理解しているはずです。
# MAGIC
# MAGIC このレッスンを開始するには、DLT UIを使用してこのノートブックをパイプラインに追加し、次に更新をトリガーします。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> このノートブックへのリンクは、[DE 4.1 - DLT UIウォークスルー]($../DE 4.1 - DLT UIウォークスルー)の**タスク#3**の**パイプライン設定を生成**セクションで印刷された手順に戻ることができます。

# COMMAND ----------

# DBTITLE 0,--i18n-fb7a717a-7921-45c3-bb1d-4c2e1bff55ab
# MAGIC %md
# MAGIC ## エラーのトラブルシューティング
# MAGIC
# MAGIC 以下の3つの関数にはそれぞれ構文エラーが含まれていますが、これらのエラーはDLTによって異なる方法で検出および報告されます。
# MAGIC
# MAGIC 一部の構文エラーは、DLTがコマンドを適切に解析できないため、**初期化**の段階で検出されます。
# MAGIC
# MAGIC 他の構文エラーは、**テーブルの設定**の段階で検出されます。
# MAGIC
# MAGIC DLTがパイプライン内のテーブルの順序を異なるステップで解決する方法のため、後の段階で最初にエラーがスローされることがあることに注意してください。
# MAGIC
# MAGIC うまくいくアプローチは、最初のデータセットから始めて最終的なデータセットに向かって1つずつテーブルを修正することです。コメントアウトされたコードは自動的に無視されるため、開発ランからコードを完全に削除せずに安全にコードを削除できます。
# MAGIC
# MAGIC 以下のコードのエラーをすぐに見つけることができても、UIからのエラーメッセージを使用してこれらのエラーの識別をガイドするようにしてください。解決策のコードは、以下のセルに続きます。

# COMMAND ----------

# TODO
import pyspark.sql.functions as F
 
source = spark.conf.get("source")
 
def status_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(f"{source}/status")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )
 
 
@dlt.table(
    table_name = "status_silver"
    )
@dlt.expect_or_drop("valid_timestamp", "status_timestamp > 1640995200")
def status_silver():
    return (
        dlt.read_stream("status_bronze")
            .drop("source_file", "_rescued_data")
    )
 
 
@dlt.table
def email_updates():
    return (
        spark.read("status_silver").alias("a")
            .join(
                dlt.read("subscribed_order_emails_v").alias("b"), 
                on="order_id"
            ).select(
                "a.*", 
                "b.email"
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-5bb5f3ef-7a9e-4ea5-a6c3-f85cac306e04
# MAGIC %md
# MAGIC ##  解決策
# MAGIC
# MAGIC 上記の各関数の正しい構文は、同じ名前のノートブック内で提供されています。
# MAGIC
# MAGIC これらのエラーに対処するためには、次のオプションがあります。
# MAGIC * 各問題を解決し、上記の問題を自分で修正する
# MAGIC * 同じ名前のSolutionsフォルダ内のSolutionsノートブックから**`# ANSWER`** セルをコピーして貼り付ける
# MAGIC * パイプラインを直接同じ名前のSolutionsノートブックを使用するように更新する
# MAGIC
# MAGIC **注意**: **`import dlt`** ステートメントを上のセルに追加するまで、他のエラーは表示されません。 
# MAGIC
# MAGIC 各クエリの問題点：
# MAGIC 1. 関数定義の前に **`@dlt.table`** デコレーターが抜けています。
# MAGIC 1. カスタムテーブル名を提供するための正しいキーワード引数は **`name`** であり、 **`table_name`** ではありません。
# MAGIC 1. パイプライン内のテーブルで読み取りを実行するには、 **`spark.read`** ではなく **`dlt.read`** を使用します。

# COMMAND ----------

# DBTITLE 0,--i18n-f8fb12d5-c515-43fc-ab55-0d1f97baf05c
# MAGIC %md
# MAGIC ## 要約
# MAGIC
# MAGIC このノートブックを通じて、以下のことを学びました:
# MAGIC * DLTの構文を識別し、トラブルシューティングする方法
# MAGIC * ノートブックを使用してDLTパイプラインを反復的に開発する方法

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
