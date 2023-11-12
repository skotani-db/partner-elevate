# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-133f57e8-6ff4-4b56-be71-9598e1513fd3
# MAGIC %md
# MAGIC # DLT Python構文の詳細
# MAGIC
# MAGIC DLTパイプラインを使用すると、1つまたは複数のノートブックを使用して複数のデータセットを組み合わせて、単一のスケーラブルなワークロードを簡単に作成できます。
# MAGIC
# MAGIC 前のノートブックでは、クラウドオブジェクトストレージからのデータ処理を通じて、クエリのシリーズを使用してデータを検証し、各ステップでレコードを豊かにする一連のDLT構文の基本機能を見直しました。このノートブックも同様にメダリオンアーキテクチャに従っていますが、いくつかの新しいコンセプトを紹介しています。
# MAGIC * 生データは顧客に関する変更データキャプチャ（CDC）情報を表します
# MAGIC * ブロンズテーブルは、再びクラウドオブジェクトストレージからのJSONデータを取り込むためにAuto Loaderを使用します
# MAGIC * 制約を強制するためにテーブルが定義され、レコードをシルバーレイヤに渡します
# MAGIC * **`dlt.apply_changes()`** は、CDCデータを自動的にシルバーレイヤにType 1 <a href="https://en.wikipedia.org/wiki/Slowly_changing_dimension" target="_blank">slowly changing dimension (SCD) table]</a>として処理するために使用されます
# MAGIC * ゴールドテーブルが定義され、このType 1テーブルの現在のバージョンから集計を計算します
# MAGIC * 別のノートブックで定義されたテーブルと結合するビューが定義されています
# MAGIC
# MAGIC ## 学習目標
# MAGIC
# MAGIC このレッスンの終わりまでに、学生は次のことに慣れているはずです：
# MAGIC * **`dlt.apply_changes()`** を使用してCDCデータを処理する
# MAGIC * ライブビューの宣言
# MAGIC * ライブテーブルの結合
# MAGIC * DLTライブラリノートブックがパイプライン内で連携する方法の説明
# MAGIC * DLTパイプラインで複数のノートブックをスケジュールする

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# DBTITLE 0,--i18n-ec15ca9a-719d-41b3-9a45-93f53bc9fb73
# MAGIC %md
# MAGIC ## Auto Loaderを使用してデータを取り込む
# MAGIC
# MAGIC 前のノートブックと同様に、Auto Loaderで構成されたデータソースに対するブロンズテーブルを定義します。
# MAGIC
# MAGIC 以下のコードに注意してください。JSONからスキーマが提供されずにまたは推論されずにデータを取り込む場合、フィールドは正しい名前を持っていますが、すべて**`STRING`**型として保存されます。
# MAGIC
# MAGIC ## テーブル名の指定
# MAGIC
# MAGIC 以下のコードは、DLTテーブル宣言の **`name`** オプションの使用例を示しています。このオプションは、結果のテーブルの名前を、テーブルが定義された関数の定義から切り離して指定できるようにします。
# MAGIC
# MAGIC 以下の例では、このオプションを使用して、テーブル名の規則（ **`<データセット名>_<データ品質>`** ）と、関数名の規則（関数が何をしているかを説明する）を満たしています（このオプションを指定しなかった場合、テーブル名は関数から推論され、 **`ingest_customers_cdc`** となりました）。

# COMMAND ----------

@dlt.table(
    name = "customers_bronze",
    comment = "Raw data from customers CDC feed"
)
def ingest_customers_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{source}/customers")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-ada9c9fb-cb58-46d8-9702-9ad16c95d821
# MAGIC %md
# MAGIC ## 品質の強制継続
# MAGIC
# MAGIC 以下のクエリでは、以下の内容を示しています。
# MAGIC * 制約が違反された場合の3つの動作オプション
# MAGIC * 複数の制約を持つクエリ
# MAGIC * 1つの制約に複数の条件を提供する
# MAGIC * 制約で組み込みのSQL関数を使用する
# MAGIC
# MAGIC データソースについて：
# MAGIC * データは**`INSERT`**、**`UPDATE`**、および**`DELETE`**操作を含むCDCフィードです。
# MAGIC * 更新および挿入操作は、すべてのフィールドに対して有効なエントリを含む必要があります。
# MAGIC * 削除操作は、タイムスタンプ、**`customer_id`**、および操作フィールド以外のすべてのフィールドに対して**`NULL`**値を含める必要があります。
# MAGIC
# MAGIC 私たちのシルバーテーブルに良いデータのみが含まれるようにするために、削除操作の予想される**`NULL`**値を無視する品質強制ルールの一連の品質強制ルールを書きます。
# MAGIC
# MAGIC 以下で各制約を詳細に説明します：
# MAGIC
# MAGIC ##### **`valid_id`**
# MAGIC この制約は、レコードが **`customer_id`** フィールドに **`NULL`** 値を含む場合、トランザクションが失敗する原因となります。
# MAGIC
# MAGIC ##### **`valid_operation`**
# MAGIC この制約は、 **`operation`** フィールドに **`NULL`** 値を含むレコードを削除します。
# MAGIC
# MAGIC ##### **`valid_address`**
# MAGIC この制約は、 **`operation`** フィールドが **`DELETE`** でない場合、住所を構成する4つのフィールドのいずれかにNULL値が含まれていないかどうかを確認します。無効なレコードに対する追加の指示がないため、違反した行はメトリクスに記録されますが、削除されません。
# MAGIC
# MAGIC ##### **`valid_email`**
# MAGIC この制約は、 **`email`** フィールドの値が有効なメールアドレスであるかどうかを正規表現パターンマッチングを使用して確認します。 **`operation`** フィールドが **`DELETE`** である場合は（これらの場合、 **`email`** フィールドに **`NULL`** 値が含まれるため）、この制約を適用しないようにロジックが含まれています。違反したレコードは削除されます。

# COMMAND ----------

@dlt.table
@dlt.expect_or_fail("valid_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "operation IS NOT NULL")
@dlt.expect("valid_name", "name IS NOT NULL or operation = 'DELETE'")
@dlt.expect("valid_adress", """
    (address IS NOT NULL and 
    city IS NOT NULL and 
    state IS NOT NULL and 
    zip_code IS NOT NULL) or
    operation = "DELETE"
    """)
@dlt.expect_or_drop("valid_email", """
    rlike(email, '^([a-zA-Z0-9_\\\\-\\\\.]+)@([a-zA-Z0-9_\\\\-\\\\.]+)\\\\.([a-zA-Z]{2,5})$') or 
    operation = "DELETE"
    """)
def customers_bronze_clean():
    return (
        dlt.read_stream("customers_bronze")
    )

# COMMAND ----------

# DBTITLE 0,--i18n-e3093247-1329-47b7-b06d-51284be37799
# MAGIC %md
# MAGIC ## **`dlt.apply_changes()`** を使用したCDCデータの処理
# MAGIC
# MAGIC DLTは、CDCフィードの処理を簡素化するための新しい構文構造を導入しています。
# MAGIC
# MAGIC  **`dlt.apply_changes()`** には以下の保証と要件があります。
# MAGIC * CDCデータのインクリメンタル/ストリーミング取り込みを実行します
# MAGIC * テーブルのプライマリキーとして1つまたは複数のフィールドを指定するための簡単な構文を提供します
# MAGIC * デフォルトの前提条件は、行には挿入および更新が含まれるということです
# MAGIC * オプションで削除を適用できます
# MAGIC * ユーザーが提供したシーケンスフィールドを使用して遅延して到着したレコードを自動的に並べ替えます
# MAGIC * **`except_column_list`** で無視する列を指定するためのシンプルな構文を使用します
# MAGIC * デフォルトでは、変更をタイプ1 SCDとして適用します
# MAGIC
# MAGIC 以下のコード：
# MAGIC * **`customers_silver`** テーブルを作成します。 **`dlt.apply_changes()`** では、変更を適用する対象のテーブルを別のステートメントで宣言する必要があります。
# MAGIC * 変更が適用される対象として **`customers_silver`** テーブルを識別します。
# MAGIC * テーブル **`customers_bronze_clean`** をソースとして指定します（**注意**：ソースは追加専用である必要があります）。
# MAGIC * プライマリキーとして **`customer_id`** を識別します。
# MAGIC * 操作が適用される順序を指定するために **`timestamp`** フィールドを指定します。
# MAGIC * **`operation`** フィールドが **`DELETE`** のレコードを削除として適用することを指定します。
# MAGIC * **`operation`** 、 **`source_file`** 、および **`_rescued_data`** 以外のすべてのフィールドを対象テーブルに追加することを示します。

# COMMAND ----------

dlt.create_target_table(
    name = "customers_silver")

dlt.apply_changes(
    target = "customers_silver",
    source = "customers_bronze_clean",
    keys = ["customer_id"],
    sequence_by = F.col("timestamp"),
    apply_as_deletes = F.expr("operation = 'DELETE'"),
    except_column_list = ["operation", "source_file", "_rescued_data"])

# COMMAND ----------

# DBTITLE 0,--i18n-43fe6e78-d4c2-46c9-9116-232b1c9bbcd9
# MAGIC %md
# MAGIC ## 適用済み変更を持つテーブルのクエリ
# MAGIC
# MAGIC **`dlt.apply_changes()`** は、デフォルトでタイプ1 SCDテーブルを作成します。つまり、各ユニークキーについて最大1つのレコードが存在し、更新は元の情報を上書きします。
# MAGIC
# MAGIC 前のセルでの操作の対象はストリーミングライブテーブルとして定義されていましたが、このテーブルではデータが更新および削除されています（したがって、ストリーミングライブテーブルソースの追加専用の要件が破られます）。したがって、下流の操作ではこのテーブルに対してストリーミングクエリを実行できません。
# MAGIC
# MAGIC このパターンにより、更新が順番外れに到着する場合、下流の結果を適切に再計算して更新を反映できるようになります。また、ソーステーブルからレコードが削除されると、これらの値はパイプラインの後のテーブルにはもう反映されないことが保証されます。
# MAGIC
# MAGIC 以下では、 **`customers_silver`** テーブルのデータからライブテーブルを作成するためのシンプルな集計クエリを定義します。

# COMMAND ----------

@dlt.table(
    comment="Total active customers per state")
def customer_counts_state():
    return (
        dlt.read("customers_silver")
            .groupBy("state")
            .agg( 
                F.count("*").alias("customer_count"), 
                F.first(F.current_timestamp()).alias("updated_at")
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-18af0840-c1e0-48b5-9f79-df03ee591aad
# MAGIC %md
# MAGIC ## DLT ビュー
# MAGIC
# MAGIC 以下のクエリは、 **`@dlt.view`** デコレータを使用して DLT ビューを定義します。
# MAGIC
# MAGIC DLT のビューは永続テーブルと異なり、関数がデコレートされたビューもストリーミング実行を継承できます。
# MAGIC
# MAGIC ビューはライブテーブルと同じ更新保証を持っていますが、クエリの結果はディスクに保存されません。
# MAGIC
# MAGIC Databricks の他の場所で使用されるビューとは異なり、DLT ビューはメタストアに永続化されず、それらはその DLT パイプラインの一部である場所からのみ参照できます（これは Databricks ノートブック内のデータフレームのスコープと似ています）。
# MAGIC
# MAGIC ビューはデータ品質を強制するために引き続き使用でき、ビューのメトリクスはテーブルと同様に収集および報告されます。
# MAGIC
# MAGIC ## 結合とノートブックライブラリ間でのテーブルの参照
# MAGIC
# MAGIC これまでに見てきたコードは、2つのソースデータセットが異なるノートブックで一連のステップを伝播していることを示しています。
# MAGIC
# MAGIC DLT は、単一の DLT パイプライン構成の一部として複数のノートブックをスケジュールすることをサポートしています。既存の DLT パイプラインを編集して追加のノートブックを追加できます。
# MAGIC
# MAGIC DLT パイプライン内では、任意のノートブックライブラリのコードは、他のノートブックライブラリで作成されたテーブルとビューを参照できます。
# MAGIC
# MAGIC 基本的に、 **`LIVE`** キーワードで参照されるデータベースのスコープは、個々のノートブックではなく、DLT パイプラインレベルであると考えることができます。
# MAGIC
# MAGIC 以下のクエリでは、 **`orders`** と **`customers`** のデータセットのシルバーテーブルを結合して新しいビューを作成します。このビューはストリーミングとして定義されていないことに注意してください。したがって、常に各顧客の現在の有効な **`email`** をキャプチャし、顧客が **`customers_silver`** テーブルから削除された後にレコードを自動的に削除します。

# COMMAND ----------

@dlt.view
def subscribed_order_emails_v():
    return (
        dlt.read("orders_silver").filter("notifications = 'Y'").alias("a")
            .join(
                dlt.read("customers_silver").alias("b"), 
                on="customer_id"
            ).select(
                "a.customer_id", 
                "a.order_id", 
                "b.email"
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-3351377c-3b34-477a-8c23-150895c2ef50
# MAGIC %md
# MAGIC ## このノートブックを DLT パイプラインに追加する
# MAGIC
# MAGIC 既存のパイプラインに他のノートブックライブラリを追加するのは、DLT UI を使用して簡単に行えます。
# MAGIC
# MAGIC 1. このコースの前半で設定した DLT パイプラインに移動します。
# MAGIC 1. 画面右上の **Settings** ボタンをクリックします。
# MAGIC 1. **Notebook Libraries** の下で、**Add notebook library** をクリックします。
# MAGIC    * ファイルピッカーを使用してこのノートブックを選択し、**Select** をクリックします。
# MAGIC 1. 更新を保存するには、**Save** ボタンをクリックします。
# MAGIC 1. 画面右上の青い **Start** ボタンをクリックして、パイプラインを更新し、新しいレコードを処理します。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> このノートブックへのリンクは、[DE 4.1 - DLT UI Walkthrough]($../DE 4.1 - DLT UI Walkthrough)の<br/>
# MAGIC **Task #2** の **Generate Pipline Configuration** セクションの **Printed Instructions** に戻っています。

# COMMAND ----------

# DBTITLE 0,--i18n-6b3c538d-c036-40e7-8739-cb3c305c09c1
# MAGIC %md
# MAGIC ## 要約
# MAGIC
# MAGIC このノートブックを通じて、以下のトピックに関して快適に感じるはずです:
# MAGIC * **`APPLY CHANGES INTO`** を使用して CDC データを処理する方法
# MAGIC * ライブビューの宣言
# MAGIC * ライブテーブルの結合
# MAGIC * DLT ライブラリノートブックがパイプライン内でどのように連携するかの説明
# MAGIC * DLT パイプライン内で複数のノートブックをスケジュールする方法
# MAGIC
# MAGIC 次のノートブックでは、パイプラインの出力を探索します。その後、DLT コードを段階的に開発およびトラブルシューティングする方法を詳しく見てみましょう。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
