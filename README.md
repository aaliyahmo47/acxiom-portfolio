# Data Analytics Portfolio
### Aaliyah Orloff | MBA, Data Analytics | Solution Management & Decision Support

This portfolio contains three projects demonstrating technical skills directly relevant to campaign data operations, decision support analysis, data quality auditing, and client reporting.

---

## Project 1 — ETL Pipeline: Marketing Campaign Data
**File:** `etl_pipeline.py`

A Python ETL pipeline that processes raw marketing campaign data through four stages — Extract, Transform, Load, and Log.

**What it does:**
- Ingests raw customer and campaign records
- Removes exact duplicates and duplicate customer IDs
- Standardizes inconsistent field values
- Validates email formats and zip codes
- Separates clean records from flagged ones
- Writes a full audit log of every transformation

**Output files:**
- `output_clean.csv` — records that passed all validation
- `output_flagged.csv` — records held for remediation
- `etl_run_log.csv` — full audit trail of the pipeline run

**Tools:** Python, Pandas

---

## Project 2 — Data Quality Audit Report
**File:** `data_quality_audit.py`

A Python audit script that runs five categories of data quality checks on any marketing dataset and produces a detailed report.

**Checks performed:**
- Completeness — null value counts per field
- Duplicates — exact row duplicates and duplicate key fields
- Format validity — email and zip code validation
- Outliers — statistical anomaly detection
- Consistency — field value standardization checks

**Output files:**
- `data_quality_report.csv` — detailed results for every check
- `data_quality_summary.csv` — high level pass/warn/fail summary

**Tools:** Python, Pandas, NumPy

---

## Project 3 — Campaign Analytics SQL Project
**File:** `campaign_analytics.sql`

A SQL project organized into three sections demonstrating data quality auditing and decision support analysis for marketing campaign data.

**Section 1 — Database Setup**
Creates three tables — customers, campaigns, and deliveries — and loads realistic marketing campaign data including intentional quality issues for demonstration purposes.

**Section 2 — Data Quality Checks**
- Total record count intake check
- Duplicate customer ID detection
- Null check across all key fields
- Segment consistency check
- Outlier detection on spend amount
- Reconciliation query to find unmatched records

**Section 3 — Decision Support Queries**
- Response rate by segment
- Total and average spend by campaign
- SLA compliance rate by campaign
- Full campaign performance summary for client review

**Tools:** SQL, SQLite

---

## Skills Demonstrated
- ETL pipeline development
- Data cleaning and transformation
- Data quality auditing and validation
- SLA compliance monitoring
- Decision support analysis and reporting
- Campaign performance analytics
- Python, Pandas, NumPy, SQL

---

## About
Three years of experience in solution management, data analysis, and campaign operations across healthcare research and engineering sectors. MBA in Data Analytics from LSU Shreveport.
