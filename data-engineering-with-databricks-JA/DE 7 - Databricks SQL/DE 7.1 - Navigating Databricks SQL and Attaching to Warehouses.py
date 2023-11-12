# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-2396d183-ba9d-4477-a92a-690506110da6
# MAGIC %md
# MAGIC
# MAGIC # Databricks SQLのナビゲーションとSQL Warehouseへのアタッチ
# MAGIC
# MAGIC * Databricks SQLに移動します
# MAGIC   * サイドバーのワークスペースオプションからSQLが選択されていることを確認します（Databricksのロゴの直下にあります）
# MAGIC * SQL Warehouseがオンでアクセス可能であることを確認します
# MAGIC   * サイドバーでSQL Warehouseに移動します
# MAGIC   * SQL Warehouseが存在し、状態が **`Running`** であれば、このSQL Warehouseを使用します
# MAGIC   * SQL Warehouseが存在し、 **`Stopped`** であれば、このオプションがある場合には **`Start`** ボタンをクリックします（ **注意** ：利用可能な最小のSQL Warehouseを起動します）
# MAGIC   * SQL Warehouseが存在せず、オプションがある場合は、 **`Create SQL Warehouse`** をクリックし、SQL Warehouseの名前を識別できるものに設定し、クラスターサイズを2X-Smallに設定します。他のオプションはデフォルトのままにします。
# MAGIC   * SQL Warehouseを作成またはアタッチする方法がない場合は、Databricks SQLで計算リソースへのアクセスをリクエストするためにワークスペース管理者に連絡する必要があります。
# MAGIC * Databricks SQLのホームページに移動します
# MAGIC   * サイドナビゲーションバーの上部にあるDatabricksロゴをクリックします
# MAGIC * **Sample dashboards** を探して **`Visit gallery`** をクリックします
# MAGIC * **Retail Revenue & Supply Chain** の横にある **`Import`** をクリックします
# MAGIC   * 利用可能なSQL Warehouseがあると仮定して、これによりダッシュボードが読み込まれ、すぐに結果が表示されるはずです
# MAGIC   * 右上の **Refresh** をクリックします（基本データは変更されていませんが、変更を取り込むために使用されるボタンです）
# MAGIC
# MAGIC # DBSQLダッシュボードの更新
# MAGIC
# MAGIC * サイドバーのナビゲーターを使用して**Dashboards**を見つけます
# MAGIC   * あなたが読み込んだばかりのサンプルダッシュボードを見つけます。これは **Retail Revenue & Supply Chain** と呼ばれ、 **`Created By`** フィールドにあなたのユーザー名が表示されているはずです。 **注意** ：右側の **My Dashboards** オプションは、ワークスペース内の他のダッシュボードをフィルタリングするのに役立ちます
# MAGIC   * ダッシュボード名をクリックして表示します
# MAGIC * **Shifts in Pricing Priorities** プロットの背後にあるクエリを表示します
# MAGIC   * プロットの上にカーソルを合わせると、3つの垂直ドットが表示されます。これをクリックします
# MAGIC   * メニューから **View Query** を選択します
# MAGIC * このプロットをポップアップするSQLコードを確認します
# MAGIC   * ソーステーブルを識別するために3階層の名前空間が使用されていることに注意してください。これはUnity Catalogでサポートされる新しい機能のプレビューです
# MAGIC   * 画面の右上にある **`Run`** をクリックしてクエリの結果をプレビューします
# MAGIC * ビジュアライゼーションを確認します
# MAGIC   * クエリの下には **Table** というタブが選択されているはずです。 **Price by Priority over Time** をクリックしてプロットのプレビューに切り替えます
# MAGIC   * 画面の下部にある **Edit Visualization** をクリックして設定を確認します
# MAGIC   * 設定を変更すると、ビジュアライゼーションにどのように影響するかを探索します
# MAGIC   * 変更を適用する場合は **Save** をクリックし、それ以外の場合は **Cancel** をクリックします
# MAGIC * クエリエディタに戻り、ビジュアライゼーション名の右側にある **Add Visualization** ボタンをクリックします
# MAGIC   * バーグラフを作成します
# MAGIC   * **X Column** を **`Date`** に設定します
# MAGIC   * **Y Column** を **`Total Price`** に設定します
# MAGIC   * **Group by** を **`Priority`** に設定します
# MAGIC   * **Stacking** を **`Stack`** に設定します
# MAGIC   * 他のすべての設定をデフォルトのままにします
# MAGIC   * **Save** をクリックします
# MAGIC * クエリエディタに戻り、このビジュアライゼーションのデフォルトの名前をクリックして編集し、ビジュアライゼーション名を **`Stacked Price`** に変更します
# MAGIC * 画面の下部で、 **`Edit Visualization`** ボタンの左側にある三つの垂直ドットをクリックします
# MAGIC   * メニューから **Add to Dashboard**
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
