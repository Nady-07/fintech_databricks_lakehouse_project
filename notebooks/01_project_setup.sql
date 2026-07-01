-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 1. Catalog & Schema Architecture
-- MAGIC We initialize our 3-level namespace boundary (`Catalog ➔ Schema ➔ Table`). This setup provisions isolated operational zones for our data as it progresses through the Medallion framework.
-- MAGIC
-- MAGIC * **Catalog:** `fintech` (Central data governance hub)
-- MAGIC * **Schemas:** 
-- MAGIC     * `bronze`: Historical raw data dump (immutable strings).
-- MAGIC     * `silver`: Cleaned, typed, and structured data tables.
-- MAGIC     * `gold`: Business-level aggregates and reporting views.

-- COMMAND ----------

USE CATALOG fintech;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Raw Ingestion Layer (Managed Volume)
-- MAGIC To stage unstructured files like raw CSV data, we create a managed Unity Catalog **Volume**. This provides a direct, high-performance file-system directory inside our secure cloud boundary.

-- COMMAND ----------

USE CATALOG fintech;
USE SCHEMA bronze;
CREATE VOLUME IF NOT EXISTS raw_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Data Ingestion Methods (Local Machine ➔ Databricks Volume)
-- MAGIC
-- MAGIC To populate our newly created `raw_data` Volume with bank source files, we utilize one of two operational pipelines depending on whether the task is ad-hoc development or a scheduled production run:
-- MAGIC
-- MAGIC ### Method A: Manual UI Upload (Best for Ad-hoc Development)
-- MAGIC
-- MAGIC 1. Click the **Catalog** icon on the far left Databricks sidebar.
-- MAGIC 2. Navigate to: `fintech` ➔ `bronze` ➔ `Volumes` ➔ `raw_data`.
-- MAGIC 3. Click the **Upload to this volume** button in the top right.
-- MAGIC 4. Drag and drop local CSV files directly into the browser window.
-- MAGIC
-- MAGIC ### Method B: Programmatic CLI Upload (Best for Local Machine Automation)
-- MAGIC
-- MAGIC For automated daily batch deliveries, we bypass the browser UI entirely. A local machine script or scheduled task pushes files directly into the cloud volume using the secure Databricks Command Line Interface (CLI).
-- MAGIC
-- MAGIC Run this command inside your local machine terminal to stream files up to the volume:
-- MAGIC
-- MAGIC ```
-- MAGIC databricks fs cp ./my_local_folder/transactions.csv dbfs:/Volumes/fintech/bronze/raw_data/
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Landing Zone Audit
-- MAGIC We execute a filesystem check directly on our Volume path to discover newly arrived CSV files and ensure our data transfer layer is communicating correctly.

-- COMMAND ----------

LIST '/Volumes/fintech/bronze/raw_data'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Staged File Data Inspection
-- MAGIC Before building permanent pipelines, we perform an ad-hoc data preview using the native `read_files` table function. 
-- MAGIC
-- MAGIC > **Bronze Safety Rule:** We explicitly set `inferSchema => false` to force all incoming data fields to load as raw string data types. This prevents type parsing errors from crashing our pipelines during raw historical landing.

-- COMMAND ----------

SELECT * FROM read_files(
    '/Volumes/fintech/bronze/raw_data/transactions.csv',
    format => 'csv',
    header => true,
    inferSchema => false
)
LIMIT 100;
