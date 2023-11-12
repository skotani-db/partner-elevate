-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-ff0a44e1-46a3-4191-b583-5c657053b1af
-- MAGIC %md
-- MAGIC # DLT SQL構文のさらなる理解
-- MAGIC
-- MAGIC DLTパイプラインを使用すると、1つまたは複数のノートブックを使用して、複数のデータセットを単一のスケーラブルなワークロードに組み合わせることが簡単になります。
-- MAGIC
-- MAGIC 前回のノートブックでは、クラウドオブジェクトストレージからのJSONデータの取り込みを含む一連のクエリを使用して、DLT構文の基本的な機能のいくつかを確認しました。このノートブックも同様にメダリオンアーキテクチャに従っており、いくつかの新しい概念を紹介しています。
-- MAGIC * 生のレコードは、顧客に関する変更データの取り込み（CDC）情報を表します
-- MAGIC * ブロンズテーブルは、再びAuto Loaderを使用してクラウドオブジェクトストレージからJSONデータを取り込みます
-- MAGIC * 制約を強制するためのテーブルが定義され、レコードがシルバーレイヤに渡される前に制約が適用されます
-- MAGIC * **`APPLY CHANGES INTO`** を使用してCDCデータを自動的にシルバーレイヤに処理し、Type 1 <a href="https://en.wikipedia.org/wiki/Slowly_changing_dimension" target="_blank">スローリーチェンジングディメンション（SCD）テーブル</a>にします
-- MAGIC * ゴールドテーブルが定義され、このType 1テーブルの現在のバージョンから集計を計算します
-- MAGIC * 別のノートブックで定義されたテーブルと結合するビューが定義されています
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC
-- MAGIC このレッスンの最後までに、学生は次のことに慣れているはずです。
-- MAGIC * **`APPLY CHANGES INTO`** を使用してCDCデータを処理する
-- MAGIC * ライブビューの宣言
-- MAGIC * ライブテーブルの結合
-- MAGIC * DLTライブラリノートブックがパイプラインで協力する方法の説明
-- MAGIC * DLTパイプラインで複数のノートブックをスケジュールする
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-959211b8-33e7-498b-8da9-771fdfb0978b
-- MAGIC %md
-- MAGIC ## Auto Loaderを使用してデータを取り込む
-- MAGIC
-- MAGIC 前回のノートブックと同様に、Auto Loaderで構成されたデータソースに対してブロンズテーブルを定義します。
-- MAGIC
-- MAGIC 以下のコードでは、スキーマを推論せずにJSONからデータを取り込むためのAuto Loaderオプションが省略されています。スキーマが提供されず、または推論されない場合、フィールドは正しい名前でありながらすべて**`STRING`**型として保存されます。
-- MAGIC
-- MAGIC 以下のコードでは、シンプルなコメントが提供され、データ取り込みのタイミングと各レコードのファイル名が追加されています。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers_bronze
COMMENT "Raw data from customers CDC feed"
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("${source}/customers", "json")

-- COMMAND ----------

-- DBTITLE 0,--i18n-43d96582-8a29-49d0-b57f-0038334f6b88
-- MAGIC %md
-- MAGIC
-- MAGIC ## データ品質の強化の続き
-- MAGIC
-- MAGIC 以下のクエリでは、以下のポイントが示されています。
-- MAGIC - 制約が違反した場合の3つの動作オプション
-- MAGIC - 複数の制約を持つクエリ
-- MAGIC - 同じ制約に複数の条件を提供
-- MAGIC - 制約内で組み込みのSQL関数を使用
-- MAGIC
-- MAGIC データソースについて：
-- MAGIC - データは **`INSERT`**、**`UPDATE`**、**`DELETE`** 操作を含むCDCフィードです。
-- MAGIC - 更新と挿入の操作はすべてのフィールドに有効なエントリを含む必要があります。
-- MAGIC - 削除操作は、タイムスタンプ、 **`customer_id`** 、および操作フィールド以外のすべてのフィールドに **`NULL`** 値を含める必要があります。
-- MAGIC
-- MAGIC 私たちのシルバーテーブルに良質なデータだけが入るようにするために、削除操作の予想される **`NULL`** 値を無視する品質強化ルールの一連を作成します。
-- MAGIC
-- MAGIC 以下では、これらの各制約を詳細に説明します。
-- MAGIC
-- MAGIC **`valid_id`**
-- MAGIC この制約は、 **`customer_id`** フィールドに **`NULL`** 値が含まれている場合、トランザクションを失敗させます。
-- MAGIC
-- MAGIC **`valid_operation`**
-- MAGIC この制約は、 **`operation`** フィールドに **`NULL`** 値が含まれているレコードを削除します。
-- MAGIC
-- MAGIC **`valid_address`**
-- MAGIC この制約は、 **`operation`** フィールドが **`DELETE`** でない場合、住所を構成する4つのフィールドのいずれかに **`NULL`** 値が含まれているかどうかを確認します。制約内の無効なレコードに対する追加の指示がないため、違反した行はメトリクスに記録されますが、削除されません。
-- MAGIC
-- MAGIC **`valid_email`**
-- MAGIC この制約は、 **`email`** フィールドの値が有効なメールアドレスであるかどうかを確認するために正規表現のパターンマッチングを使用します。 **`operation`** フィールドが **`DELETE`** である場合はこれを適用しないようにロジックが組まれています（なぜならこれらは **`email`** フィールドに **`NULL`** 値を持つことになるからです）。違反したレコードは削除されます。
-- MAGIC

-- COMMAND ----------

CREATE STREAMING LIVE TABLE customers_bronze_clean
(CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_name EXPECT (name IS NOT NULL or operation = "DELETE"),
CONSTRAINT valid_address EXPECT (
  (address IS NOT NULL and 
  city IS NOT NULL and 
  state IS NOT NULL and 
  zip_code IS NOT NULL) or
  operation = "DELETE"),
CONSTRAINT valid_email EXPECT (
  rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') or 
  operation = "DELETE") ON VIOLATION DROP ROW)
AS SELECT *
  FROM STREAM(LIVE.customers_bronze)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5766064f-1619-4468-ac05-2176451e11c0
-- MAGIC %md
-- MAGIC 上記のクエリでは、CDCデータを処理するための新しい構文構造である **`APPLY CHANGES INTO`** が紹介されています。
-- MAGIC
-- MAGIC **`APPLY CHANGES INTO`** は、以下の保証と要件があります。
-- MAGIC * CDCデータの増分/ストリーミング取り込みを実行します。
-- MAGIC * 1つまたは複数のフィールドをテーブルの主キーとして指定するためのシンプルな構文を提供します。
-- MAGIC * デフォルトの仮定は、行には挿入と更新が含まれているということです。
-- MAGIC * オプションで削除を適用できます。
-- MAGIC * ユーザーが提供したシーケンスキーを使用して、遅延して到着するレコードを自動的に並べ替えます。
-- MAGIC * **`EXCEPT`** キーワードを使用して無視する列を指定するためのシンプルな構文を使用します。
-- MAGIC * 変更をType 1 SCDとして適用するというデフォルトがあります。
-- MAGIC
-- MAGIC 上記のコードは、次のことを行っています。
-- MAGIC * **`customers_silver`** テーブルを作成します。 **`APPLY CHANGES INTO`** は、変更が適用される対象のテーブルを別のステートメントで宣言する必要があります。
-- MAGIC * **`customers_silver`** テーブルを変更が適用される対象として識別します。
-- MAGIC * ストリーミングソースとして **`customers_bronze_clean`** テーブルを指定します。
-- MAGIC * **`customer_id`** を主キーとして指定します。
-- MAGIC * **`operation`** フィールドが **`DELETE`** の場合は削除として適用されるレコードを指定します。
-- MAGIC * オペレーションが適用される方法を指定するために **`timestamp`** フィールドを指定します。
-- MAGIC * **`operation`** 、 **`source_file`** 、および **`_rescued_data`** 以外のすべてのフィールドを対象テーブルに追加するように指定します。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers_silver;

APPLY CHANGES INTO LIVE.customers_silver
  FROM STREAM(LIVE.customers_bronze_clean)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY timestamp
  COLUMNS * EXCEPT (operation, source_file, _rescued_data)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5956effc-3fb5-4473-b2e7-297e5a3e1103
-- MAGIC %md
-- MAGIC
-- MAGIC **`APPLY CHANGES INTO`** は、デフォルトでType 1 SCDテーブルを作成するようになっており、つまり、各ユニークキーには最大1つのレコードしかなく、更新は元の情報を上書きします。
-- MAGIC
-- MAGIC 前のセルでの操作の対象がストリーミングライブテーブルとして定義されていましたが、このテーブルではデータが更新および削除されています（したがって、ストリーミングライブテーブルソースの追加専用要件が壊れています）。 したがって、ダウンストリームの操作はこのテーブルに対してストリーミングクエリを実行することはできません。
-- MAGIC
-- MAGIC このパターンにより、更新が順番外れで到着した場合、ダウンストリームの結果を正しく再計算して更新を反映できるようになります。 また、ソーステーブルからレコードが削除された場合、これらの値はパイプライン内の後のテーブルにはもはや反映されないようになります。
-- MAGIC
-- MAGIC 以下では、 **`customers_silver`** テーブルのデータからライブテーブルを作成するための簡単な集計クエリを定義しています。
-- MAGIC

-- COMMAND ----------

CREATE LIVE TABLE customer_counts_state
  COMMENT "Total active customers per state"
AS SELECT state, count(*) as customer_count, current_timestamp() updated_at
  FROM LIVE.customers_silver
  GROUP BY state

-- COMMAND ----------

-- DBTITLE 0,--i18n-cd430cf6-927e-4a2e-a68d-ba87b9fdf3f6
-- MAGIC %md
-- MAGIC
-- MAGIC 以下のクエリでは、 **`VIEW`** キーワードで **`TABLE`** を置き換えることで、DLTビューを定義しています。
-- MAGIC
-- MAGIC DLTのビューは永続テーブルとは異なり、オプションで **`STREAMING`** として定義することができます。
-- MAGIC
-- MAGIC ビューはライブテーブルと同じ更新の保証を持っていますが、クエリの結果はディスクに保存されません。
-- MAGIC
-- MAGIC Databricksの他の場所で使用されるビューとは異なり、DLTビューはメタストアに永続されないため、それらは所属するDLTパイプライン内からのみ参照できます（これはほとんどのSQLシステムの一時ビューと同じスコーピングです）。
-- MAGIC
-- MAGIC ビューは引き続きデータ品質を強制するために使用でき、ビューのメトリクスはテーブルのように収集および報告されます。
-- MAGIC
-- MAGIC ## Joins and Referencing Tables Across Notebook Libraries
-- MAGIC
-- MAGIC これまでに確認したコードは、2つのソースデータセットが異なるノートブックで一連の手順を伝播するのを示しています。
-- MAGIC
-- MAGIC DLTは、1つのDLTパイプライン構成の一部として複数のノートブックをスケジュールすることをサポートしています。既存のDLTパイプラインを編集して追加のノートブックを追加できます。
-- MAGIC
-- MAGIC DLTパイプライン内では、任意のノートブックライブラリで作成されたテーブルとビューを参照できます。
-- MAGIC
-- MAGIC 基本的には、 **`LIVE`** キーワードによって参照されるスキーマのスコープを個々のノートブックではなくDLTパイプラインレベルと考えることができます。
-- MAGIC
-- MAGIC 以下のクエリでは、 **`orders`** および **`customers`** データセットからのシルバーテーブルを結合して新しいビューを作成しています。このビューはストリーミングとして定義されていないため、常に各顧客の現在の有効な **`email`** がキャプチャされ、 **`customers_silver`** テーブルから削除された後はレコードが自動的に削除されます。
-- MAGIC

-- COMMAND ----------

CREATE LIVE VIEW subscribed_order_emails_v
  AS SELECT a.customer_id, a.order_id, b.email 
    FROM LIVE.orders_silver a
    INNER JOIN LIVE.customers_silver b
    ON a.customer_id = b.customer_id
    WHERE notifications = 'Y'

-- COMMAND ----------

-- DBTITLE 0,--i18n-d4a1dd79-e8e6-4d18-bf00-c5d49cd04b04
-- MAGIC %md
-- MAGIC
-- MAGIC ## このノートブックをDLTパイプラインに追加
-- MAGIC
-- MAGIC 既存のパイプラインに追加のノートブック ライブラリを追加するのは、DLT UI を使用して簡単に行えます。
-- MAGIC
-- MAGIC 1. このコースで既に構成したDLTパイプラインに移動します
-- MAGIC 1. 画面右上の **Settings** ボタンをクリックします
-- MAGIC 1. **Notebook Libraries** の下で、**Add notebook library** をクリックします
-- MAGIC    * ファイルピッカーを使用してこのノートブックを選択し、**Select** をクリックします
-- MAGIC 1. 更新を保存するために **Save** ボタンをクリックします
-- MAGIC 1. 画面右上の青い **Start** ボタンをクリックして、パイプラインを更新し、新しいレコードを処理します
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> このノートブックへのリンクは、[DE 4.1 - DLT UI Walkthrough]($../DE 4.1 - DLT UI Walkthrough)の<br/>
-- MAGIC **Task #2** の **Generate Pipeline Configuration** セクションの指示の中にあります
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c0ccf2a-333a-4d7c-bfef-b4b25e56b3ca
-- MAGIC %md
-- MAGIC
-- MAGIC ## 概要
-- MAGIC
-- MAGIC このノートブックを確認することで、以下の作業に慣れるようになるでしょう：
-- MAGIC * **`APPLY CHANGES INTO`** を使用して CDC データを処理する
-- MAGIC * ライブビューを宣言する
-- MAGIC * ライブテーブルを結合する
-- MAGIC * DLT ライブラリ ノートブックがどのように連携するかを説明する
-- MAGIC * DLT パイプラインで複数のノートブックをスケジュールする
-- MAGIC
-- MAGIC 次のノートブックでは、パイプラインの出力を探索します。その後、DLT コードの反復的な開発とトラブルシューティングの方法を見ていきます。
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
