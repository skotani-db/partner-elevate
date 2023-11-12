-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-54a6a2f6-5787-4d60-aea6-e39627954c96
-- MAGIC %md
-- MAGIC # DLT SQL構文のトラブルシューティング
-- MAGIC
-- MAGIC 2つのノートブックでパイプラインの設定と実行のプロセスを経た後、今度は3つ目のノートブックを開発して追加することをシミュレートします。
-- MAGIC
-- MAGIC **パニックにならないでください！** これから問題が発生します。
-- MAGIC
-- MAGIC 以下に提供されているコードには、意図的な小さな構文エラーが含まれています。これらのエラーをトラブルシューティングすることで、DLTコードの反復的な開発方法と構文のエラーを特定する方法を学びます。
-- MAGIC
-- MAGIC このレッスンは、コード開発とテストの堅牢なソリューションを提供することを目的としたものではありません。むしろ、DLTを始めたばかりで、馴染みのない構文に苦労しているユーザーを支援することを目的としています。
-- MAGIC
-- MAGIC ## 学習目標
-- MAGIC このレッスンの終わりまでに、学生は以下について快適に感じることができるはずです：
-- MAGIC * DLT構文の特定とトラブルシューティング
-- MAGIC * ノートブックでのDLTパイプラインの反復的な開発

-- COMMAND ----------

-- DBTITLE 0,--i18n-dee15416-56fd-48d7-ae3c-126175503a9b
-- MAGIC %md
-- MAGIC ## このノートブックをDLTパイプラインに追加する
-- MAGIC
-- MAGIC このコースのこの段階では、2つのノートブックライブラリで構成されたDLTパイプラインを持っているはずです。
-- MAGIC
-- MAGIC このパイプラインを通じて複数のデータバッチを処理し、新しい実行をトリガーし、追加ライブラリを追加する方法を理解しているはずです。
-- MAGIC
-- MAGIC このレッスンを始めるには、DLT UIを使用してこのノートブックをパイプラインに追加するプロセスを行い、その後アップデートをトリガーしてください。
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> このノートブックへのリンクは [DE 4.1 - DLT UIウォークスルー]($../DE 4.1 - DLT UI Walkthrough) に戻って<br/>
-- MAGIC **Generate Pipeline Configuration** セクションの **Task #3** の印刷された指示に記載されています。

-- COMMAND ----------

-- DBTITLE 0,--i18n-96beb691-44d9-4871-9fa5-fcccc3e13616
-- MAGIC %md
-- MAGIC ## エラーのトラブルシューティング
-- MAGIC
-- MAGIC 以下の3つのクエリそれぞれに構文エラーが含まれていますが、DLTによってこれらのエラーは異なる方法で検出され報告されます。
-- MAGIC
-- MAGIC 一部の構文エラーは、**初期化中**の段階で検出されます。これはDLTがコマンドを適切に解析できないためです。
-- MAGIC
-- MAGIC 他の構文エラーは、**テーブルの設定中**の段階で検出されます。
-- MAGIC
-- MAGIC パイプライン内のテーブルの順序をDLTが異なるステップで解決する方法のため、時には後の段階のエラーが先に表示されることがあります。
-- MAGIC
-- MAGIC うまくいくアプローチは、最初のデータセットから始めて最終的なものに向かって一度に一つのテーブルを修正することです。コメントアウトされたコードは自動的に無視されるので、開発実行からコードを完全に削除せずに安全に取り除くことができます。
-- MAGIC
-- MAGIC 以下のコードでエラーをすぐに見つけられたとしても、UIのエラーメッセージを使ってこれらのエラーの特定をガイドするようにしてみてください。解決策のコードは以下のセルに続きます。

-- COMMAND ----------

-- TODO
CREATE OR REFRESH STREAMING TABLE status_bronze
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("${source}/status", "json");

CREATE OR REFRESH STREAMING LIVE TABLE status_silver
(CONSTRAINT valid_timestamp EXPECT (status_timestamp > 1640995200) ON VIOLATION DROP ROW)
AS SELECT * EXCEPT (source_file, _rescued_data)
FROM LIVE.status_bronze;

CREATE OR REFRESH LIVE TABLE email_updates
AS SELECT a.*, b.email
FROM status_silver a
INNER JOIN subscribed_order_emails_v b
ON a.order_id = b.order_id;

-- COMMAND ----------

-- DBTITLE 0,--i18n-492901e2-38fe-4a8c-a05d-87b9dedd775f
-- MAGIC %md
-- MAGIC ## 解決策
-- MAGIC
-- MAGIC 上記の各関数の正しい構文は、Solutionsフォルダにある同名のノートブックで提供されています。
-- MAGIC
-- MAGIC これらのエラーに対処するには、いくつかのオプションがあります：
-- MAGIC * 各問題を一つずつ取り組み、上記の問題を自分で修正する
-- MAGIC * 同名のSolutionsノートブックの **`# ANSWER`** セルから解決策をコピー＆ペーストする
-- MAGIC * 同名のSolutionsノートブックを直接使用するようにパイプラインを更新する
-- MAGIC
-- MAGIC 各クエリの問題点：
-- MAGIC 1. create文から **`LIVE`** キーワードが欠けています
-- MAGIC 1. from句に **`STREAM`** キーワードが欠けています
-- MAGIC 1. from句によって参照されるテーブルから **`LIVE`** キーワードが欠けています

-- COMMAND ----------

-- DBTITLE 0,--i18n-54e251d6-b8f8-45c2-82df-60e22b127135
-- MAGIC %md
-- MAGIC ## まとめ
-- MAGIC
-- MAGIC このノートブックを確認することで、以下の点について快適に感じるはずです：
-- MAGIC * DLT構文の特定とトラブルシューティング
-- MAGIC * ノートブックを使用してDLTパイプラインを反復的に開発する

-- COMMAND ----------

-- -- ANSWER
-- CREATE OR REFRESH STREAMING LIVE TABLE status_bronze
-- AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
-- FROM cloud_files("${source}/status", "json");

-- CREATE OR REFRESH STREAMING LIVE TABLE status_silver
-- (CONSTRAINT valid_timestamp EXPECT (status_timestamp > 1640995200) ON VIOLATION DROP ROW)
-- AS SELECT * EXCEPT (source_file, _rescued_data)
-- FROM STREAM(LIVE.status_bronze);

-- CREATE OR REFRESH LIVE TABLE email_updates
-- AS SELECT a.*, b.email
-- FROM LIVE.status_silver a
-- INNER JOIN LIVE.subscribed_order_emails_v b
-- ON a.order_id = b.order_id;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
