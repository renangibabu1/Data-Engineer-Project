# Credit Risk Analytics

Project code for the Home Credit Risk Analytics dashboard.

## End-to-end Databricks workflow

The project combines local CSV profiling with a Databricks data pipeline and a
Streamlit dashboard that connects to Databricks:

1. **Profile the source files (local, optional).** Run
   [`01_data_exploration.py`](notebooks/01_data_exploration.py) to scan the
   original CSVs in chunks, check candidate keys and missing values, and write
   `reports/data_profile.json`. By default, the script reads from
   `~/Downloads/home-credit-default-risk`. This step does not upload data to
   Databricks.
2. **Land the source files.** Upload the source CSVs listed below to the Unity
   Catalog volume `/Volumes/credit_risk/bronze/raw_files`. The ingestion job
   reads from this volume; the placeholder files under `data/raw/` are not
   used.
3. **Build Bronze.** Run
   [`02_bronze_ingestion.py`](notebooks/02_bronze_ingestion.py) with the
   `home-credit-dev` Databricks CLI profile. It reads the volume with
   Databricks Connect and writes raw-string Delta tables to `credit_risk.bronze`.
4. **Build Silver and Gold.** Run
   [`03_silver_transformation.py`](notebooks/03_silver_transformation.py),
   then [`04_gold_analytics.py`](notebooks/04_gold_analytics.py). Silver
   provides typed, validated tables; Gold provides dashboard summaries and
   segment aggregates.
5. **Run the dashboard.** Start `dashboard/app.py` locally with the same
   Databricks CLI profile configured. The app uses Databricks Connect to read
   the Silver and Gold tables. Default-cohort summaries use Gold; filtered
   aggregates are recalculated from Silver.

Run the Databricks stages in order—Bronze, Silver, then Gold—before starting
the dashboard. The source CSVs must already be present in the Unity Catalog
volume, and the CLI profile must be authenticated for the configured workspace.

## Bronze ingestion

The ingestion script reads the CSVs from the Unity Catalog volume
`/Volumes/credit_risk/bronze/raw_files` and writes source columns as strings to
Delta tables in `credit_risk.bronze`. It adds `_source_file` and `_ingested_at`
metadata columns and verifies each table's row count. The column-description
CSV is read with `cp1252` encoding.

| Source file | Bronze table | Rows verified |
| --- | --- | ---: |
| `application_train.csv` | `application_train` | 307,511 |
| `application_test.csv` | `application_test` | 48,744 |
| `bureau.csv` | `bureau` | 1,716,428 |
| `bureau_balance.csv` | `bureau_balance` | 27,299,925 |
| `credit_card_balance.csv` | `credit_card_balance` | 3,840,312 |
| `installments_payments.csv` | `installments_payments` | 13,605,401 |
| `POS_CASH_balance.csv` | `pos_cash_balance` | 10,001,358 |
| `previous_application.csv` | `previous_application` | 1,670,214 |
| `sample_submission.csv` | `sample_submission` | 48,744 |
| `HomeCredit_columns_description.csv` | `homecredit_columns_description` | 219 |

Run it from the workspace root in PowerShell after Databricks CLI authentication:

```powershell
$env:DATABRICKS_CONFIG_PROFILE = 'home-credit-dev'
.\.venv\Scripts\python.exe `
  '.\Credit_Risk_Analytics\notebooks\02_bronze_ingestion.py'
```

The script overwrites each target table on rerun so the same source snapshot
does not append duplicate rows. The placeholder files in `data/raw/` are not
used by this ingestion.

## Silver tables

`03_silver_transformation.py` reads `credit_risk.bronze.application_train`,
casts all 122 source columns using the types verified from the uploaded CSV,
trims string values, and preserves blank values as null. It fails on invalid
numeric casts, null or duplicate `SK_ID_CURR` keys, or `TARGET` values outside
0 and 1. It writes `credit_risk.silver.application_train` and verifies that
the row count is unchanged.

The same script transforms `credit_risk.bronze.bureau` into
`credit_risk.silver.bureau`, validates its bureau-record key, and preserves
decimal day values such as `DAYS_CREDIT_ENDDATE` as doubles.

It also builds typed Silver tables for `previous_application` and
`installments_payments`. Previous-application IDs are checked for uniqueness;
installment records retain their source grain and are checked for required
keys.

## Gold application analytics

`04_gold_analytics.py` reads the Silver table and writes:

- `credit_risk.gold.application_overview`: application and target counts/rate,
  average and median credit amount, average income, annuity, and goods price,
  plus TARGET=1-only averages, recorded-tenure outcome rate, and the full-cohort
  income-credit correlation.
- `credit_risk.gold.application_segment_metrics`: counts, `TARGET=1` rate,
  average credit, and average income by contract type, gender, education,
  income type, occupation, and family status.
- `credit_risk.gold.bureau_overview`: bureau record and customer counts,
  active/closed credit counts, totals, average credit age, mean summed bureau
  credit per linked applicant, and TARGET=1 rate among applicants with bureau
  history.
- `credit_risk.gold.bureau_status_metrics`,
  `credit_risk.gold.bureau_overdue_distribution`,
  `credit_risk.gold.bureau_duration_distribution`, and
  `credit_risk.gold.bureau_education_metrics`: pre-aggregated credit status,
  overdue, credit-age, and applicant-education metrics.
- `credit_risk.gold.previous_application_overview`,
  `credit_risk.gold.previous_application_status_metrics`, and
  `credit_risk.gold.previous_application_purpose_metrics`: prior application
  totals, decision status, and requested loan purpose.
- `credit_risk.gold.installments_overview` and
  `credit_risk.gold.installments_status_metrics`: installment totals,
  payment timing, and recorded shortfall. Late means payment-entry day is
  later than the scheduled installment day; missing payment dates are kept
  separate.

The bureau Gold tables include only bureau records linked to
`application_train`. The dashboard distinguishes bureau records from unique
applicants; filtered views recalculate from Silver.

The source column description defines `TARGET=1` as a client with payment
difficulties. Gold names this outcome `target_1`; it does not assume that it
means a general or final loan default.

Run Silver and then Gold after Bronze has completed:

```powershell
.\.venv\Scripts\python.exe `
  '.\Credit_Risk_Analytics\notebooks\03_silver_transformation.py'
.\.venv\Scripts\python.exe `
  '.\Credit_Risk_Analytics\notebooks\04_gold_analytics.py'
```

Both scripts overwrite their target tables on rerun and verify reconciliation
before reporting success.

## Streamlit dashboard

`dashboard/app.py` provides all ten pages: Executive Overview, Customer
Demographics, Loan Application Analysis, Risk Analysis, Income & Employment,
Credit Bureau Analysis, Previous Loan Applications, Repayment & Installments,
ML Payment-Difficulty Explorer, and Reports & Data Download. The app uses
shared HTML/CSS styling, a compact top filter bar, responsive KPI cards, and a
navy sidebar. Global filters cover contract type, gender, income, application
credit amount, and `TARGET`. Default cohort metrics use Gold summaries;
filtered aggregates read Silver. Results are cached for five minutes; use
**Refresh data** to force a reload.

User-facing risk labels retain the source definition: `TARGET=1` means recorded
payment difficulties, not a confirmed general or final default. Likewise,
`DAYS_EMPLOYED=365243` is shown as tenure unavailable, not unemployment.
Income-credit correlation uses the full filtered cohort; plotted points are a
sample. Bureau metrics distinguish record-level amounts from per-applicant
totals and report the payment-difficulty rate only among applicants with bureau
history. Missing installment payments are treated as zero only in the
shortfall calculation and are not represented as proof that no payment
occurred.

The ML page trains a sampled, exploratory logistic-regression baseline from
the current training cohort. It is not validated for lending decisions. The
report page provides filtered CSV samples bounded to 100 rows; use Databricks
for full-table exports.

Run it from the workspace root in PowerShell after Databricks CLI
authentication:

```powershell
$env:DATABRICKS_CONFIG_PROFILE = 'home-credit-dev'
.\.venv\Scripts\streamlit.exe run `
  '.\Credit_Risk_Analytics\dashboard\app.py'
```

Run Bronze ingestion, then Silver transformation and Gold analytics before
starting the app. It labels `TARGET=1` as payment difficulties, matching the
source data dictionary, and treats `DAYS_EMPLOYED=365243` as unavailable
rather than as confirmed unemployment.

See the workspace-level [roadmap](../ROADMAP.md) for the remaining Databricks
implementation steps.
