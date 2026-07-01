-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1. Landing Zone File Verification
-- MAGIC
-- MAGIC Before executing any data ingestion pipelines, we run a file system check on our cloud storage Volume. This allows us to verify that all the required raw bank CSV extracts have successfully landed from our local machine and are ready to be processed.

-- COMMAND ----------

LIST '/Volumes/fintech/bronze/raw_data'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. Individual Table Ingestion (Manual Proof-of-Concept)
-- MAGIC
-- MAGIC To validate our data ingestion logic, we first write explicit, individual SQL scripts for each core bank entity. This baseline approach allows us to inspect the raw incoming schemas for each file, verify column counts, and ensure our audit trail metadata fields apply correctly.
-- MAGIC
-- MAGIC ### Core Bank Entities Ingested:
-- MAGIC * `transactions` — Core customer ledger records.
-- MAGIC * `customer` — Demographic and account identity profiles.
-- MAGIC * `date` — Financial accounting calendar lookup dimension.
-- MAGIC * `fraud_aml_alerts` — Compliance and Anti-Money Laundering transaction flags.
-- MAGIC * `geography` — Regional branch and customer location attributes.
-- MAGIC * `loan_application` — Staged retail and commercial loan application funnels.
-- MAGIC * `loans` — Active accounts containing active borrowing debt.
-- MAGIC
-- MAGIC > **Development Note:** While writing individual scripts is effective for initial schema discoveries and prototyping, it becomes inefficient and hard to maintain when managing dozens of changing files over time.

-- COMMAND ----------

USE CATALOG fintech;
USE SCHEMA bronze;
CREATE OR REPLACE TABLE transactions
USING delta
AS
SELECT 
    *,
    _metadata.file_path AS file_path,
    current_timestamp() AS ingest_ts
FROM 
    read_files(
        '/Volumes/fintech/bronze/raw_data/transactions.csv',
        format => 'csv',
        header => true,
        inferSchema => false
        );

-- COMMAND ----------

USE CATALOG fintech;
USE SCHEMA bronze;
CREATE OR REPLACE TABLE customer
USING delta
AS
SELECT
    *,
    _metadata.file_path AS file_path,
    current_timestamp() AS ingest_ts
FROM
    read_files(
        '/Volumes/fintech/bronze/raw_data/customer.csv',
        format => 'csv',
        header => true,
        inferSchema => false
    );

-- COMMAND ----------

USE CATALOG fintech;
USE SCHEMA bronze;
CREATE OR REPLACE TABLE date
USING delta
AS
SELECT
    *,
    _metadata.file_path AS file_path,
    current_timestamp() AS ingest_ts
FROM
    read_files(
        '/Volumes/fintech/bronze/raw_data/date.csv',
        format => 'csv',
        header => true,
        inferSchema => false
    );


-- COMMAND ----------

USE CATALOG fintech;
USE SCHEMA bronze;
CREATE OR REPLACE TABLE fraud_aml_alerts
USING delta
AS
SELECT
    *,
    _metadata.file_path AS file_path,
    current_timestamp() AS ingest_ts
FROM
    read_files(
        '/Volumes/fintech/bronze/raw_data/fraud_aml_alerts.csv',
        format => 'csv',
        header => true,
        inferSchema => false
    );

-- COMMAND ----------

USE CATALOG fintech;
USE SCHEMA bronze;
CREATE OR REPLACE TABLE geography
USING delta
AS 
SELECT
    *,
    _metadata.file_path AS file_path,
    current_timestamp() AS ingest_ts
FROM
    read_files(
        '/Volumes/fintech/bronze/raw_data/geography.csv',
        format => 'csv',
        header => true,
        inferSchema => false
    );

-- COMMAND ----------

USE CATALOG fintech;
USE SCHEMA bronze;
CREATE OR REPLACE TABLE loan_applications
USING delta
AS
SELECT
    *,
    _metadata.file_path AS file_path,
    current_timestamp() AS ingest_ts
FROM
    read_files(
        '/Volumes/fintech/bronze/raw_data/loan_applications.csv',
        format => 'csv',
        header => true,
        inferSchema => false
    );

-- COMMAND ----------

USE CATALOG fintech;
USE SCHEMA bronze;
CREATE OR REPLACE TABLE loans
USING delta 
AS
SELECT
    *,
    _metadata.file_path AS file_path,
    current_timestamp() AS ingest_ts
FROM
    read_files(
        '/Volumes/fintech/bronze/raw_data/loans.csv',
        format => 'csv',
        header => true,
        inferSchema => false
    );


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. Production Ingestion Optimization (Dynamic Automation Loop)
-- MAGIC
-- MAGIC To scale our architecture and follow the **DRY (Don't Repeat Yourself)** engineering principle, we replace the manual individual scripts with a **Python-wrapped Spark SQL pipeline**. 
-- MAGIC
-- MAGIC This automated loop transforms our notebook into a self-configuring, production-ready ingestion engine.
-- MAGIC
-- MAGIC ### **How the Optimization Pipeline Works:**
-- MAGIC 1. **Directory Sweeping:** `dbutils.fs.ls()` scans our raw storage Volume to discover files dynamically.
-- MAGIC 2. **File Isolation:** The pipeline isolates and identifies files matching the target `.csv` extension.
-- MAGIC 3. **Dynamic Table Scoping:** The extension is stripped from the file name, and the result is encapsulated in SQL backticks (`` ` ``). This ensures names containing spaces or hyphens are generated safely without failing.
-- MAGIC 4. **Bulk Execution:** `spark.sql()` compiles and executes the `CREATE OR REPLACE TABLE` logic for every file found in a single notebook execution pass.
-- MAGIC
-- MAGIC ### **Business Value:**
-- MAGIC If our upstream financial platforms drop 5 new transaction history or risk reporting files into the landing zone tomorrow, this notebook will adapt instantly, auto-generating the new Bronze Delta tables on its next run without requiring any manual developer code updates.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import re
-- MAGIC
-- MAGIC raw_path = '/Volumes/fintech/bronze/raw_data/'
-- MAGIC bronze_schema = 'fintech.bronze'
-- MAGIC
-- MAGIC files = dbutils.fs.ls(raw_path)
-- MAGIC
-- MAGIC for file in files:
-- MAGIC     if file.name.endswith(".csv"):
-- MAGIC         table_name = file.name.replace(".csv", "")
-- MAGIC         table_name = re.sub(r"[^a-zA-Z0-9_]", "_", table_name).lower()
-- MAGIC
-- MAGIC         sql_statement = f"""
-- MAGIC         CREATE OR REPLACE TABLE {bronze_schema}.`{table_name}`
-- MAGIC         USING DELTA
-- MAGIC         AS
-- MAGIC         SELECT
-- MAGIC           *,
-- MAGIC           _metadata.file_path AS file_path,
-- MAGIC           current_timestamp() AS ingest_ts
-- MAGIC         FROM read_files(
-- MAGIC           '{raw_path}',
-- MAGIC           format => 'csv',
-- MAGIC           header => true,
-- MAGIC           inferSchema => false
-- MAGIC         )
-- MAGIC         """
-- MAGIC         spark.sql(sql_statement)
-- MAGIC
-- MAGIC         print(f"Successfully created table: {bronze_schema}.`{table_name}`")
