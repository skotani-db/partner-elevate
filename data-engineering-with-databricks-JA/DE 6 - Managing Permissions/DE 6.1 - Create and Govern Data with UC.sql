-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-b5d0b5d6-287e-4dcd-8a4b-a0fe2b12b615
-- MAGIC %md
-- MAGIC # Unity Catalogを使用してデータオブジェクトを作成および管理する
-- MAGIC
-- MAGIC このノートブックでは、次のことを学びます：
-- MAGIC * カタログ、スキーマ、テーブル、ビュー、およびユーザー定義関数の作成
-- MAGIC * これらのオブジェクトへのアクセス制御
-- MAGIC * テーブル内の列と行を保護するための動的ビューの使用
-- MAGIC * Unity Catalog内のさまざまなオブジェクトに関する権限の調査

-- COMMAND ----------

-- DBTITLE 0,--i18n-63c92b27-28a0-41f8-8761-a9ec5bb574e0
-- MAGIC %md
-- MAGIC ## 前提条件
-- MAGIC
-- MAGIC このラボに従いたい場合、次のことが必要です：
-- MAGIC * カタログを作成および管理するためのメタストア管理者権限を持っていること
-- MAGIC * ユーザーがアクセスできるSQLデータベースを持っていること
-- MAGIC   * ユーザーが上記に言及されているアクセス権を持っている場合、ノートブックを参照してください：Unity Catalogアクセスのためのコンピュートリソースの作成

-- COMMAND ----------

-- DBTITLE 0,--i18n-67495bbd-0f5b-4f81-9974-d5c6360fb86c
-- MAGIC %md
-- MAGIC ## セットアップ
-- MAGIC
-- MAGIC 以下のセルを実行してセットアップを行います。共有トレーニング環境での競合を避けるため、これにより専用に使用するための一意のカタログ名が生成されます。後で使用します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-06.1

-- COMMAND ----------

-- DBTITLE 0,--i18n-a0720484-97de-4742-9c38-6c5bd312f3d7
-- MAGIC %md
-- MAGIC ## Unity Catalogの三段階の名前空間
-- MAGIC
-- MAGIC SQLの経験がある場合、スキーマ内のテーブルまたはビューをアドレスするための従来の二段階の名前空間についてはおそらく既知のものでしょう。次のクエリの例に示すように、次のように表されます。
-- MAGIC
-- MAGIC     SELECT * FROM myschema.mytable;
-- MAGIC
-- MAGIC Unity Catalogは、階層に**カタログ**という概念を導入します。スキーマのコンテナとして、カタログはデータを分離する新しい方法を提供します。カタログはいくつでも作成でき、それぞれにいくつでもスキーマ（スキーマの概念はUnity Catalogによって変更されず、スキーマにはテーブル、ビュー、ユーザー定義関数などのデータオブジェクトが含まれます）を含めることができます。
-- MAGIC
-- MAGIC この追加のレベルを扱うために、Unity Catalogの完全なテーブル/ビュー参照は三段階の名前空間を使用します。次のクエリはこれを示しています。
-- MAGIC
-- MAGIC     SELECT * FROM mycatalog.myschema.mytable;
-- MAGIC
-- MAGIC これは多くのユースケースで便利です。たとえば：
-- MAGIC
-- MAGIC * 組織内のビジネスユニット（営業、マーケティング、人事など）に関連するデータを分離する
-- MAGIC * SDLC要件（dev、staging、prodなど）を満たす
-- MAGIC * 内部での一時的なデータセットを含むサンドボックスを設立する

-- COMMAND ----------

-- DBTITLE 0,--i18n-aa4c68dc-6dc3-4aca-a352-affc98ac8089
-- MAGIC %md
-- MAGIC ### 新しいカタログを作成
-- MAGIC メタストアに新しいカタログを作成しましょう。変数**`${DA.my_new_catalog}`**は、セットアップセルで表示され、ユーザー名に基づいて生成された一意の文字列を含んでいます。
-- MAGIC
-- MAGIC 以下の**`CREATE`**ステートメントを実行し、左のサイドバーにある**Data**アイコンをクリックして、この新しいカタログが作成されたことを確認してください。

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ${DA.my_new_catalog}

-- COMMAND ----------

-- DBTITLE 0,--i18n-e1f478c8-bbf2-4368-9cdd-e130d2fb7410
-- MAGIC %md
-- MAGIC ### デフォルトのカタログを選択
-- MAGIC
-- MAGIC SQLの開発者は、デフォルトのスキーマを選択するために**`USE`**ステートメントにも慣れているかもしれません。これにより、常に指定する必要がなくなり、クエリが短縮されます。ネームスペースの追加レベルを扱う際にこの便益を拡張するために、Unity Catalogは以下の例に示すように、言語に2つの追加ステートメントを追加しています。
-- MAGIC
-- MAGIC     USE CATALOG mycatalog;
-- MAGIC     USE SCHEMA myschema;  
-- MAGIC     
-- MAGIC 新しく作成したカタログをデフォルトとして選択しましょう。これで、スキーマへの参照は、カタログへの参照によって明示的に上書きされない限り、このカタログに存在すると想定されます。

-- COMMAND ----------

USE CATALOG ${DA.my_new_catalog}

-- COMMAND ----------

-- DBTITLE 0,--i18n-bc4ad8ed-6550-4457-92f7-d88d22709b3c
-- MAGIC %md
-- MAGIC ### 新しいスキーマの作成と使用
-- MAGIC 次に、この新しいカタログ内にスキーマを作成しましょう。このスキーマは一意のカタログを使用しているため、新しいスキーマの一意な名前を生成する必要はありません。また、デフォルトのスキーマとして設定しましょう。これで、明示的に2つまたは3つのレベルの参照で上書きされない限り、データへの参照は作成したカタログとスキーマにあると想定されます。
-- MAGIC
-- MAGIC 以下のコードを実行し、左サイドバーの**Data**アイコンをクリックして、このスキーマが新しく作成したカタログに作成されたことを確認してください。

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS example;
USE SCHEMA example

-- COMMAND ----------

-- DBTITLE 0,--i18n-87b6328c-4641-4d40-b66c-f166a4166902
-- MAGIC %md
-- MAGIC ### テーブルとビューの設定
-- MAGIC
-- MAGIC 必要なコンテナがすべて用意されたので、テーブルとビューを設定しましょう。この例では、合成患者の心拍数データを使用して、*silver* マネージドテーブルと、患者ごとに日別に心拍数データを平均化する *gold* ビューを作成し、データを埋めます。
-- MAGIC
-- MAGIC 以下のセルを実行し、左サイドバーの**Data**アイコンをクリックして、*example* スキーマの内容を探索してください。以下のテーブルまたはビューの名前を指定するとき、デフォルトのカタログとスキーマを選択したため、3つのレベルを指定する必要はありません。

-- COMMAND ----------

CREATE OR REPLACE TABLE heartrate_device (device_id INT, mrn STRING, name STRING, time TIMESTAMP, heartrate DOUBLE);

INSERT INTO heartrate_device VALUES
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:01:58.000+0000", 54.0122153343),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:02:55.000+0000", 92.5136468131),
  (37, "65300842", "Samuel Hughes", "2020-02-01T00:08:58.000+0000", 52.1354807863),
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:16:51.000+0000", 54.6477014191),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:18:08.000+0000", 95.033344842);
  
SELECT * FROM heartrate_device

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)
);
SELECT * FROM agg_heartrate

-- COMMAND ----------

-- DBTITLE 0,--i18n-3ab35bc3-e89b-48d0-bcef-91a268688a19
上記のテーブルへのクエリは、データの所有者であるため、期待どおりに機能します。つまり、クエリ対象のデータオブジェクトを所有しています。ビューへのクエリも機能します。ビューとそれが参照しているテーブルの両方の所有者であるため、これらのリソースにアクセスするためにオブジェクトレベルのアクセス許可は必要ありません。

-- COMMAND ----------

-- DBTITLE 0,--i18n-9ce42a62-c95a-45fb-9d6b-a4d3725110e7
-- MAGIC %md
-- MAGIC ##  _account users_ グループ
-- MAGIC
-- MAGIC Unity Catalogが有効になっているアカウントには、_account users_ グループが存在します。このグループには、Databricksアカウントからワークスペースに割り当てられたすべてのユーザーが含まれています。我々はこのグループを使用して、異なるグループのユーザーに対するデータオブジェクトのアクセスがどのように異なるかを示します。

-- COMMAND ----------

-- DBTITLE 0,--i18n-29eeb0ee-94e0-4b95-95be-a8776d20dc6c
-- MAGIC %md
-- MAGIC
-- MAGIC ## データオブジェクトにアクセス権を付与する
-- MAGIC
-- MAGIC Unity Catalogは、デフォルトで明示的なアクセス許可モデルを採用しています。つまり、含まれる要素からのアクセス許可は暗黙的には継承されません。したがって、データオブジェクトにアクセスするためには、ユーザーはすべての含まれる要素、つまり含まれるスキーマとカタログに対する**USAGE**許可が必要です。
-- MAGIC
-- MAGIC さて、*account users* グループのメンバーが *gold* ビューをクエリできるようにしましょう。これを行うには、以下のアクセス許可を付与する必要があります：
-- MAGIC 1. カタログとスキーマへの USAGE 許可
-- MAGIC 1. データオブジェクト（たとえばビュー）への SELECT 許可

-- COMMAND ----------

-- GRANT USAGE ON CATALOG ${DA.my_new_catalog} TO `account users`;
-- GRANT USAGE ON SCHEMA example TO `account users`;
-- GRANT SELECT ON VIEW agg_heartrate to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-dabadee1-5624-4330-855d-d0c0de1f76b4
-- MAGIC %md
-- MAGIC ### ビューのクエリ
-- MAGIC
-- MAGIC データオブジェクトの階層が設定され、適切な許可が与えられたら、*gold* ビューでクエリを実行しましょう。
-- MAGIC
-- MAGIC 私たち全員が**account users** グループのメンバーであるため、このグループを使用して設定を確認し、変更を加えた場合の影響を観察できます。
-- MAGIC
-- MAGIC 1. 左上隅にあるアプリ切り替えボタンをクリックして開きます。
-- MAGIC 2. **SQL** を右クリックし、**新しいタブでリンクを開く** を選択します。
-- MAGIC 3. **クエリ** ページに移動し、**クエリの作成** をクリックします。
-- MAGIC 4. *Unity Catalog access用のコンピューティングリソースの作成* デモの手順に従って作成された共有のSQLデータウェアハウスを選択します。
-- MAGIC 5. このノートブックに戻り、引き続き従ってください。プロンプトが表示されたら、Databricks SQLセッションに切り替えてクエリを実行します。
-- MAGIC
-- MAGIC 以下のセルは、変数やデフォルトのカタログとスキーマが設定されていない環境で実行するため、ビューに対してすべてのレベルを指定する完全修飾クエリステートメントを生成します。生成されたクエリをDatabricks SQLセッションで実行してください。account usersがビューにアクセスするための適切な許可がすべて揃っているため、出力は以前に*gold* ビューをクエリした際に見たものに似ているはずです。
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT * FROM ${DA.my_new_catalog}.example.patient_heart_rate_daily;
-- MAGIC ```

-- COMMAND ----------

SELECT "SELECT * FROM ${DA.my_new_catalog}.example.agg_heartrate" AS Query

-- COMMAND ----------

-- DBTITLE 0,--i18n-7fb51344-7eb4-49a2-916c-6cdee06e534a
-- MAGIC %md
-- MAGIC ### シルバーテーブルをクエリする
-- MAGIC Databricks SQLセッション内の同じクエリに戻り、*gold* を*silver* に置き換えてクエリを実行しましょう。今回はクエリが失敗します。なぜなら、*silver* テーブルに対するアクセス許可を設定していないからです。
-- MAGIC
-- MAGIC *gold* をクエリすることはできます。なぜなら、ビューによって表されるクエリは基本的にビューの所有者として実行されるためです。この重要なプロパティにより、いくつかの興味深いセキュリティユースケースが可能になります。この方法で、ビューはユーザーに対してデータそのものへのアクセス権を提供せずに、機密データの制限されたビューを提供できます。近々、これについて詳しく見ていきます。
-- MAGIC
-- MAGIC 現時点では、Databricks SQLセッションで*silver* クエリを閉じて破棄できます。これ以上使用しないからです。
-- MAGIC
-- MAGIC Is there anything else you would like to translate or any other tasks I can assist you with?

-- COMMAND ----------

-- DBTITLE 0,--i18n-bea5cb66-3642-45b1-8906-906588b99b06
-- MAGIC %md
-- MAGIC ### ユーザー定義関数の作成とアクセス権の付与
-- MAGIC
-- MAGIC Unity Catalogは、スキーマ内でユーザー定義関数を管理することができます。以下のコードは、文字列の最後の2文字以外をマスクするシンプルな関数をセットアップし、それを試してみます。再度、データの所有者であるため、アクセス権は必要ありません。

-- COMMAND ----------

CREATE OR REPLACE FUNCTION mask(x STRING)
  RETURNS STRING
  RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2)
); 
SELECT mask('sensitive data') AS data

-- COMMAND ----------

-- DBTITLE 0,--i18n-d0945f12-4045-471b-a319-376f5f8f25dd
-- MAGIC %md
-- MAGIC
-- MAGIC *account users* グループのメンバーに、関数を実行するためには、関数に対する **EXECUTE** 権限が必要です。また、前述したスキーマとカタログに対する必要な **USAGE** 権限も必要です。

-- COMMAND ----------

-- GRANT EXECUTE ON FUNCTION mask to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-e74e14a8-d372-44e0-a301-94cc046efd29
-- MAGIC %md
-- MAGIC ### 関数を実行する
-- MAGIC
-- MAGIC さて、この関数を Databricks SQL で試してみましょう。以下で生成された完全修飾クエリ文を新しいクエリに貼り付けて、この関数を Databricks SQL で実行します。関数にアクセスするための適切な権限がすべて設定されているため、出力は先ほど見たものと似ているはずです。

-- COMMAND ----------

SELECT "SELECT ${DA.my_new_catalog}.example.mask('sensitive data') AS data" AS Query

-- COMMAND ----------

-- DBTITLE 0,--i18n-8697f10a-6924-4bac-9c9a-7d91285eb9f5
-- MAGIC %md
-- MAGIC ## ダイナミックビューを使用してテーブルの列と行を保護する
-- MAGIC
-- MAGIC Unity Catalogは、ビューの処理によって、ビューがテーブルへのアクセスを保護する機能を提供します。ユーザーは、ビューへのアクセス権を付与され、ソーステーブルへの直接アクセスを提供することなく、データを操作、変換、または隠すビューにアクセスできます。
-- MAGIC
-- MAGIC ダイナミックビューは、テーブル内の列と行に対する細かいアクセス制御を行うための機能を提供し、クエリを実行するプリンシパルに応じて条件付きで適用できます。ダイナミックビューは、標準ビューの拡張機能で、次のようなことができます。
-- MAGIC * 列の値を一部非表示にしたり、完全に非表示にする
-- MAGIC * 特定の基準に基づいて行を省略する
-- MAGIC
-- MAGIC ダイナミックビューを使用したアクセス制御は、ビューの定義内で関数を使用することで実現されます。これらの関数には次のものが含まれます。
-- MAGIC * **`current_user()`**: ビューをクエリしているユーザーのメールアドレスを返します
-- MAGIC * **`is_account_group_member()`**: ビューをクエリしているユーザーが指定されたグループのメンバーである場合、TRUEを返します
-- MAGIC
-- MAGIC 注意: レガシー関数 **`is_member()`** を使用しないでください。これはワークスペースレベルのグループを参照するもので、Unity Catalogのコンテキストでは適切なプラクティスではありません。

-- COMMAND ----------

-- DBTITLE 0,--i18n-8fc52e53-927e-4d6b-a340-7082c94d4e6e
-- MAGIC %md
-- MAGIC ### 列を非表示にする
-- MAGIC
-- MAGIC 仮に、アカウントユーザーには*gold*ビューからの集計データトレンドを見ることができるようにしたいが、患者のPII情報を開示したくないとしましょう。**`is_account_group_member()`** を使用して、*mrn*および*name*列を非表示にするようにビューを再定義しましょう。
-- MAGIC
-- MAGIC 注意: これは一般的なベストプラクティスと必ずしも一致しない、単純なトレーニング例です。本番システムでは、特定のグループのメンバーでないすべてのユーザーの列値を非表示にする、よりセキュアなアプローチが適しています。

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('account users') THEN 'REDACTED'
    ELSE mrn
  END AS mrn,
  CASE WHEN
    is_account_group_member('account users') THEN 'REDACTED'
    ELSE name
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)

-- COMMAND ----------

-- DBTITLE 0,--i18n-01381247-0c36-455b-b64c-22df863d9926
-- MAGIC %md
-- MAGIC
-- MAGIC 権限を再発行します。
-- MAGIC

-- COMMAND ----------

-- GRANT SELECT ON VIEW agg_heartrate to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-0bf9bd34-2351-492c-b6ef-e48241339d0f
-- MAGIC
-- MAGIC %md
-- MAGIC
-- MAGIC それでは、Databricks SQLに戻り、*gold*ビューのクエリを再実行しましょう。以下のセルを実行して、このクエリを生成します。
-- MAGIC
-- MAGIC *mrn*および*name*の列値が非表示にされていることがわかります。

-- COMMAND ----------

SELECT "SELECT * FROM ${DA.my_new_catalog}.example.agg_heartrate" AS Query

-- COMMAND ----------

-- DBTITLE 0,--i18n-0bb3c639-daf8-4b46-9c28-cafd32a12917
-- MAGIC %md
-- MAGIC ### 行を制限する
-- MAGIC
-- MAGIC 次に、列を集計して非表示にするのではなく、ソースから行を単純にフィルタリングするビューが必要だとします。 *device_id* が30未満の行のみを通過させるビューを作成するために、同じ **`is_account_group_member()`** 関数を適用しましょう。行のフィルタリングは、条件を **`WHERE`** 句として適用することによって行われます。

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

-- DBTITLE 0,--i18n-69bc283c-f426-4ba2-b296-346c69de1c20
-- MAGIC %md
-- MAGIC
-- MAGIC 権限を再発行します。

-- COMMAND ----------

-- GRANT SELECT ON VIEW agg_heartrate to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-c4f3366a-4992-4d5a-b597-e140091a8d00
-- MAGIC %md
-- MAGIC
-- MAGIC 上記のビューをクエリするすべてのユーザーにとって、5つのレコードが表示されます。今度はDatabricks SQLに戻り、*gold*ビューのクエリを再実行します。その結果、1つのレコードが見当たらないことがわかります。欠落しているレコードには、フィルターによってキャッチされた*device_id*の値が含まれています。

-- COMMAND ----------

-- DBTITLE 0,--i18n-dffccf13-5205-44d5-beab-4d08b085f54a
-- MAGIC %md
-- MAGIC ### データのマスキング
-- MAGIC 動的ビューの最後のユースケースは、データのマスキング、またはデータの一部を隠すことです。最初の例では、列を完全に隠しました。マスキングは原理的には同じですが、完全に置き換えるのではなく、一部のデータを表示しています。そして、このシンプルな例では、以前に作成した*mask()*ユーザー定義関数を使用して*mrn*列をマスクしますが、SQLにはデータをさまざまな方法でマスクできる組み込みのデータ操作関数の包括的なライブラリが提供されているため、それらを活用するのが良い習慣です。

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('account users') THEN mask(mrn)
    ELSE mrn
  END AS mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

-- DBTITLE 0,--i18n-735fa71c-0d31-4484-9736-30dc098dee8d
-- MAGIC %md
-- MAGIC
-- MAGIC 権限を再度付与します

-- COMMAND ----------

-- GRANT SELECT ON VIEW agg_heartrate to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-9c3d9b8f-17ce-498f-b6dd-dfb470855086
-- MAGIC %md
-- MAGIC
-- MAGIC グループのメンバーでないユーザーにとって、これは変更されていないレコードを表示します。Databricks SQLに戻り、goldビューのクエリを再実行してください。mrn列のすべての値がマスクされます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-3e809063-ad1b-4a2e-8cc3-b87492c8ffc3
-- MAGIC %md
-- MAGIC
-- MAGIC ## オブジェクトの探索
-- MAGIC
-- MAGIC データオブジェクトと権限を調査するためのいくつかのSQLステートメントを試してみましょう。まず、*examples*スキーマに存在するオブジェクトの概要を把握しましょう。

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW VIEWS

-- COMMAND ----------

-- DBTITLE 0,--i18n-40599e0f-47f9-46da-be2b-7b856da0cba1
-- MAGIC %md
-- MAGIC 上記の2つのステートメントでは、デフォルトのスキーマを指定していないため、選択したデフォルトを利用しています。代わりに、　**`SHOW TABLES IN example`**　のようなステートメントを使用してもよいでしょう。
-- MAGIC
-- MAGIC さて、階層を上げてカタログ内のスキーマを確認してみましょう。再び、デフォルトのカタログが選択されていることを利用しています。より明示的にするためには、　**`SHOW SCHEMAS IN ${DA.my_new_catalog}`**　のようなステートメントを使用することができます。

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

-- DBTITLE 0,--i18n-943178c2-9ab3-4941-ac1a-70b63103ecb7
-- MAGIC %md
-- MAGIC もちろん、*example* スキーマは以前に作成したものです。*default* スキーマは、新しいカタログを作成する際にSQLの規則に従ってデフォルトで作成されます。
-- MAGIC
-- MAGIC 最後に、メタストア内のカタログをリストアップしましょう。

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- DBTITLE 0,--i18n-e8579147-7d9a-4b27-b0e9-3ab6c4ec9a0c
-- MAGIC %md
-- MAGIC 表示されるエントリが予想よりも多いかもしれません。最低限、以下のエントリが表示されます。
-- MAGIC * *dbacademy_* プレフィックスで始まるカタログ：以前に作成したカタログです。
-- MAGIC * *hive_metastore*：メタストア内の実際のカタログではなく、ワークスペースローカルのHiveメタストアの仮想表現です。ワークスペースローカルのテーブルとビューにアクセスするために使用します。
-- MAGIC * *main*：新しいメタストアごとにデフォルトで作成されるカタログです。
-- MAGIC * *samples*：Databricksが提供するサンプルデータセットを表示するための仮想カタログです。
-- MAGIC
-- MAGIC メタストア内の過去のアクティビティに応じて、その他のカタログも存在するかもしれません。

-- COMMAND ----------

-- DBTITLE 0,--i18n-9477b0a2-3099-4fca-bb7d-f6e298ce254b
-- MAGIC %md
-- MAGIC ### 権限
-- MAGIC 次に、 **`SHOW GRANTS`** を使用してアクセス許可を調査し、*gold* ビューから上に向かって進んでみましょう。

-- COMMAND ----------

-- SHOW GRANTS ON VIEW agg_heartrate

-- COMMAND ----------

-- DBTITLE 0,--i18n-98d1534e-a51c-45f6-83d0-b99549ccc279
-- MAGIC %md
-- MAGIC 現時点では、私たちが設定した **SELECT** の許可しかありません。次に、*silver* の許可を確認しましょう。

-- COMMAND ----------

-- SHOW GRANTS ON TABLE heartrate_device

-- COMMAND ----------

-- DBTITLE 0,--i18n-591b3fbb-7ed3-4e88-b435-3750b212521d
-- MAGIC %md
-- MAGIC 現在、このテーブルには許可がありません。データ所有者である私たちだけが、このテーブルに直接アクセスできます。*gold* ビューにアクセス権がある人は、間接的にこのテーブルにアクセスできる権限を持っています。
-- MAGIC
-- MAGIC 次に、含まれるスキーマを見てみましょう。

-- COMMAND ----------

-- SHOW GRANTS ON SCHEMA example

-- COMMAND ----------

-- DBTITLE 0,--i18n-ab78d60b-a596-4a19-80ae-a5d742169b6c
-- MAGIC %md
-- MAGIC 現在、以前に設定した**USAGE**権限が表示されています。
-- MAGIC
-- MAGIC さて、カタログを調べてみましょう。

-- COMMAND ----------

-- SHOW GRANTS ON CATALOG ${DA.my_new_catalog}

-- COMMAND ----------

-- DBTITLE 0,--i18n-62f7e069-7260-4a48-9676-16088958cffc
-- MAGIC %md
-- MAGIC 同様に、少し前に付与した**USAGE**も表示されます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-200fe251-2176-46ca-8ecc-e725d8c9da01
-- MAGIC %md
-- MAGIC ## アクセスの取り消し
-- MAGIC
-- MAGIC データガバナンスプラットフォームには、以前に発行された許可を取り消す能力がないと不完全です。まず、*mask()* 関数へのアクセスを調べることから始めましょう。

-- COMMAND ----------

-- SHOW GRANTS ON FUNCTION mask

-- COMMAND ----------

-- DBTITLE 0,--i18n-9495b822-96bd-4fe5-aed7-9796ffd722d0
-- MAGIC %md
-- MAGIC さて、この許可を取り消しましょう。

-- COMMAND ----------

-- REVOKE EXECUTE ON FUNCTION mask FROM `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-c599d523-08cc-4d39-994d-ce919799c276
-- MAGIC %md
-- MAGIC 今度はアクセスを再び確認しましょう。これは今は空です。

-- COMMAND ----------

-- SHOW GRANTS ON FUNCTION mask

-- COMMAND ----------

-- DBTITLE 0,--i18n-a47d66a4-aa65-470b-b8ea-d8e7c29fce95
-- MAGIC %md
-- MAGIC
-- MAGIC Databricks SQLセッションに戻り、*gold*ビューに対するクエリを再実行してみてください。これは以前と同じように機能します。これに驚かれたでしょうか？なぜか、またはなぜでないかを考えてみてください。
-- MAGIC
-- MAGIC 覚えておいてください、ビューは実質的にはその所有者として実行されており、その所有者は関数とソーステーブルも所有しています。ビューの例が、ビューの所有者がテーブルに直接アクセスする必要がなかったのと同様に、関数も同じ理由で機能し続けます。
-- MAGIC
-- MAGIC さて、何か違うことを試してみましょう。カタログから**USAGE**を取り消すことで、アクセス許可チェーンを破りましょう。

-- COMMAND ----------

-- REVOKE USAGE ON CATALOG ${DA.my_new_catalog} FROM `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-b1483973-1ef7-4b6d-9a04-931c53947148
-- MAGIC %md
-- MAGIC
-- MAGIC Databricks SQLに戻り、*gold*クエリを再実行すると、ビューとスキーマに適切なアクセス許可があるにもかかわらず、階層の上位で欠落している権限により、このリソースへのアクセスが中断されることがわかります。これはUnity Catalogの明示的なアクセス許可モデルが動作している例です。アクセス許可は暗示されることも継承されることもありません。

-- COMMAND ----------

-- DBTITLE 0,--i18n-1ffb00ac-7663-4206-84b6-448b50c0efe2
-- MAGIC %md
-- MAGIC ## クリーンアップ
-- MAGIC 前に作成したカタログを削除するために、以下のセルを実行しましょう。 **`CASCADE`** 修飾子は、カタログとそれに含まれる要素をすべて削除します。
-- MAGIC

-- COMMAND ----------

USE CATALOG hive_metastore;
DROP CATALOG IF EXISTS ${DA.my_new_catalog} CASCADE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
