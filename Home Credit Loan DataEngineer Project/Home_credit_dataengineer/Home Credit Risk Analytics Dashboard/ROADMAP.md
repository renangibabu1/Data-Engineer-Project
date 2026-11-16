# Home Credit Risk Analytics Dashboard Roadmap

## Goal

Build a reliable credit-risk analytics dashboard from the project CSV files, using Databricks for ingestion, processing, storage, and scheduling. Azure Data Factory is not part of this plan.

## End-to-end flow

**Local CSVs → Databricks storage → PySpark ingestion → Bronze Delta tables → cleaned Silver tables → Gold analytics with SQL/PySpark → dashboard → Databricks Workflows**

## Phases

### 1. Define the questions and metric meanings

- List the decisions the dashboard should support, such as application volume, risk rates by segment, loan amounts, and repayment trends.
- Confirm the meaning of target, status, and outcome columns from the dataset documentation before naming or calculating risk metrics.
- Define each metric’s population, numerator, denominator, and time period.

**Deliverable:** a short metric and dashboard requirements list.

### 2. Inventory and profile the CSVs

For each file, record its name, size, row count, columns, what one row represents, candidate keys, and relationships to other files. Check data types, missing values, duplicate keys, and date formats.

Pay particular attention to table grain. A customer or application can have multiple related records, so joining raw files directly may multiply rows and inflate dashboard metrics.

**Deliverable:** a source inventory and documented table grains and keys.

### 3. Make source files available to Databricks

- Keep the original CSVs unchanged.
- For a small initial batch, upload files to a Unity Catalog Volume.
- For larger or recurring inputs, use the workspace’s approved cloud storage location and Unity Catalog access.
- Confirm that Databricks can read the files before building transformations.

**Deliverable:** accessible source files and a repeatable description of where new files belong.

### 4. Set up the data layers

Use Unity Catalog to organize the project. A starting layout could be:

- Catalog: `credit_risk`
- Schemas: `bronze`, `silver`, and `gold`
- Volume or governed cloud location: source CSVs

Keep the layers purposeful:

- **Bronze:** source-faithful Delta tables, with ingestion metadata such as source filename and ingestion timestamp.
- **Silver:** standardized, typed, cleaned, and validated data.
- **Gold:** dashboard-ready tables or views with stable, documented metric definitions.

Adjust catalog and schema names to match the workspace’s existing governance conventions.

### 5. Ingest one CSV into Bronze

Create a Databricks notebook using PySpark to read one CSV and write it to Delta. Preserve source values and capture basic ingestion metadata. Compare the Bronze row count and columns with the source.

**Exit check:** the first file loads repeatably and its source-to-Bronze counts reconcile.

### 6. Clean and validate into Silver

Use PySpark to standardize column names and data types, parse dates, and handle missing or invalid values according to documented rules. Check duplicates and key validity, and make validation failures visible.

Before joining related data, verify each table’s grain and keys. Aggregate one-to-many sources to the required level before joining when necessary.

**Exit check:** Silver tables pass defined quality checks and joins do not inflate application-level counts.

### 7. Build Gold analytics

Use SQL for readable analytical queries and PySpark where transformations or data volume call for it. Build a small number of dashboard-ready summaries, starting with an application-level summary and validated segment metrics.

Document metric definitions alongside the Gold outputs so dashboard results agree with ad hoc SQL.

**Exit check:** Gold totals and metrics reconcile to independently checked SQL queries.

### 8. Build the dashboard

Choose the dashboard surface once the workspace and audience needs are clear:

- **Databricks SQL dashboard:** simplest if the dashboard should remain within Databricks.
- **Streamlit:** suitable for a custom Python interface; connect it to the Gold layer through an approved Databricks SQL connection.

A first version can include an overview, segment comparisons, repayment/history summaries where supported by the data, and data-quality indicators. Add filters that support defined questions, and avoid exposing unnecessary customer-level sensitive data.

**Exit check:** dashboard totals match Gold/SQL results, and filters and empty-result cases behave correctly.

### 9. Schedule and monitor with Databricks

Use Databricks Jobs/Workflows to run the stages in order:

**Ingest → validate and clean → build Gold tables → refresh dashboard data**

Begin with manual runs while developing. Add a schedule when inputs and refresh expectations are known. Make failed runs visible through run logs and failure notifications. No Azure Data Factory is planned.

**Exit check:** a workflow run completes end to end, and failures can be identified and investigated.

### 10. Verify and share responsibly

- Reconcile dashboard counts and metrics with source counts and SQL queries.
- Test missing values, duplicate keys, joins, filters, and empty results.
- Confirm metric labels and definitions are accurate.
- Apply appropriate workspace permissions and avoid publishing unnecessary sensitive details.

## Recommended first milestone

Take **one CSV** through the full path: upload, Bronze Delta, Silver cleaning and checks, one Gold summary, and one dashboard view. Reconcile the numbers at each stage. Then add the remaining files one at a time, documenting their grains and relationships before joining them.

## Decisions to confirm before implementation

- Which CSV files are in scope and what each row represents.
- The verified meaning of risk/outcome columns and the initial metrics.
- The Databricks catalog, schema, and storage conventions available in the workspace.
- Whether the dashboard should be a Databricks SQL dashboard or a Streamlit app.
- Expected refresh frequency and who should have access.
