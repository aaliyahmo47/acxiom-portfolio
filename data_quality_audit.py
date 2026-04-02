import pandas as pd
import numpy as np
from datetime import datetime

# ============================================================
# DATA QUALITY AUDIT REPORT — Marketing Campaign Dataset
# Aaliyah Orloff
# Produces a full quality audit report on any dataset —
# checks completeness, duplicates, format validity, and
# statistical outliers. Outputs a summary report CSV.
# ============================================================

def load_data():
    print("[ LOADING ] Reading dataset...")
    data = {
        "customer_id":   [1001,1002,1003,1004,1005,1006,1007,1008,1009,1010,1011,1002,1013,None,1015],
        "first_name":    ["James","Tanya","Marcus","Lisa","Derek","Sandra","Paul","Kevin","Priya","Omar","Beth","Tanya","Chris",None,"Rachel"],
        "last_name":     ["Holloway","Reed","Sullivan","Chang","Webb","Morton","Brooks","Nash","Patel","Diaz","Ford","Reed","Lane","Smith","Green"],
        "email":         ["j.holloway@email.com","tanya.reed@email.com","marcus.s@email.com","lisa.chang@email.com","derek.webb@email.com","not-an-email","paul.b@email.com","kevin.nash@email.com","priya.p@email.com","omar.d@email.com","beth.ford@email.com","tanya.reed@email.com","chris.lane@email.com","bademail","rachel.g@email.com"],
        "zip_code":      ["72015","71101","72034","90210","71105","ABCDE","72015","72032","10001","75001",None,"71101","72019","99999","72011"],
        "segment":       ["Prospect","Active","Lapsed","Active","prospect","Active","Lapsed","PROSPECT","Active","Lapsed","Active","Active","prospect","Lapsed","Active"],
        "campaign_id":   ["C001","C002","C001","C003","C002","C001","C003","C001","C003","C002",None,"C002","C001","C003","C002"],
        "response_flag": [1,0,1,1,0,1,0,1,1,0,1,0,1,0,1],
        "record_date":   ["2025-01-15","2025-01-15","2025-01-16","2025-01-16","2025-01-17","2025-01-17","2025-01-18","2025-01-19","2025-01-19","2025-01-20","2025-01-20","2025-01-15","2025-01-21","2025-01-21","2025-01-22"],
        "spend_amount":  [120.50, 85.00, 200.00, 95.75, 110.00, 88.50, 175.00, 92.00, 5000.00, 105.00, 130.00, 85.00, 115.00, 78.00, 145.00]
    }
    df = pd.DataFrame(data)
    print(f"    Records loaded: {len(df)}")
    return df


def check_completeness(df):
    print("\n[ CHECK 1 ] Completeness — checking for missing values...")
    results = []
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_pct = round((null_count / len(df)) * 100, 1)
        status = "PASS" if null_count == 0 else "WARN" if null_pct < 10 else "FAIL"
        results.append({
            "check": "Completeness",
            "field": col,
            "issue_count": null_count,
            "issue_pct": f"{null_pct}%",
            "status": status,
            "detail": f"{null_count} null value(s) found" if null_count > 0 else "No nulls"
        })
        if null_count > 0:
            print(f"    {col}: {null_count} null(s) ({null_pct}%) — {status}")
        else:
            print(f"    {col}: clean")
    return results


def check_duplicates(df):
    print("\n[ CHECK 2 ] Duplicates — checking for duplicate records...")
    results = []
    exact_dupes = df.duplicated().sum()
    id_dupes = df.duplicated(subset="customer_id").sum()
    results.append({
        "check": "Duplicates",
        "field": "all_fields",
        "issue_count": int(exact_dupes),
        "issue_pct": f"{round((exact_dupes/len(df))*100,1)}%",
        "status": "PASS" if exact_dupes == 0 else "FAIL",
        "detail": f"{exact_dupes} exact duplicate row(s)" if exact_dupes > 0 else "No exact duplicates"
    })
    results.append({
        "check": "Duplicates",
        "field": "customer_id",
        "issue_count": int(id_dupes),
        "issue_pct": f"{round((id_dupes/len(df))*100,1)}%",
        "status": "PASS" if id_dupes == 0 else "FAIL",
        "detail": f"{id_dupes} duplicate customer_id(s)" if id_dupes > 0 else "No duplicate IDs"
    })
    print(f"    Exact duplicate rows: {exact_dupes}")
    print(f"    Duplicate customer IDs: {id_dupes}")
    return results


def check_format_validity(df):
    print("\n[ CHECK 3 ] Format validity — checking emails and zip codes...")
    results = []
    email_mask = df["email"].str.contains(r"^[\w\.\+\-]+@[\w\-]+\.\w+$", na=False)
    bad_emails = (~email_mask).sum()
    results.append({
        "check": "Format",
        "field": "email",
        "issue_count": int(bad_emails),
        "issue_pct": f"{round((bad_emails/len(df))*100,1)}%",
        "status": "PASS" if bad_emails == 0 else "WARN" if bad_emails <= 2 else "FAIL",
        "detail": f"{bad_emails} invalid email format(s)" if bad_emails > 0 else "All emails valid"
    })
    zip_mask = df["zip_code"].str.match(r"^\d{5}$", na=False)
    bad_zips = (~zip_mask).sum()
    results.append({
        "check": "Format",
        "field": "zip_code",
        "issue_count": int(bad_zips),
        "issue_pct": f"{round((bad_zips/len(df))*100,1)}%",
        "status": "PASS" if bad_zips == 0 else "WARN" if bad_zips <= 2 else "FAIL",
        "detail": f"{bad_zips} invalid zip code(s)" if bad_zips > 0 else "All zip codes valid"
    })
    print(f"    Invalid emails: {bad_emails}")
    print(f"    Invalid zip codes: {bad_zips}")
    return results


def check_outliers(df):
    print("\n[ CHECK 4 ] Outliers — checking spend_amount for anomalies...")
    results = []
    mean = df["spend_amount"].mean()
    std = df["spend_amount"].std()
    outliers = df[np.abs(df["spend_amount"] - mean) > (3 * std)]
    results.append({
        "check": "Outliers",
        "field": "spend_amount",
        "issue_count": len(outliers),
        "issue_pct": f"{round((len(outliers)/len(df))*100,1)}%",
        "status": "PASS" if len(outliers) == 0 else "WARN",
        "detail": f"{len(outliers)} outlier(s) detected beyond 3 standard deviations (mean: ${round(mean,2)}, std: ${round(std,2)})" if len(outliers) > 0 else "No outliers detected"
    })
    print(f"    Mean spend: ${round(mean,2)}")
    print(f"    Std deviation: ${round(std,2)}")
    print(f"    Outliers found: {len(outliers)}")
    if len(outliers) > 0:
        print(f"    Outlier values: {outliers['spend_amount'].tolist()}")
    return results


def check_consistency(df):
    print("\n[ CHECK 5 ] Consistency — checking segment field standardization...")
    results = []
    unique_vals = df["segment"].unique().tolist()
    non_standard = [v for v in unique_vals if v != v.title()]
    results.append({
        "check": "Consistency",
        "field": "segment",
        "issue_count": len(non_standard),
        "issue_pct": f"{round((len(non_standard)/len(unique_vals))*100,1)}%",
        "status": "PASS" if len(non_standard) == 0 else "WARN",
        "detail": f"Non-standard values found: {non_standard}" if non_standard else "All values consistent"
    })
    print(f"    Unique segment values: {unique_vals}")
    print(f"    Non-standard values: {non_standard if non_standard else 'None'}")
    return results


def generate_report(all_results, total_records):
    print("\n[ REPORT ] Generating audit report...")
    report_df = pd.DataFrame(all_results)
    report_df.to_csv("~/data_quality_report.csv", index=False)

    pass_count = len(report_df[report_df["status"] == "PASS"])
    warn_count = len(report_df[report_df["status"] == "WARN"])
    fail_count = len(report_df[report_df["status"] == "FAIL"])
    total_issues = report_df["issue_count"].sum()
    overall = "PASS" if fail_count == 0 else "FAIL"

    summary = [{
        "report_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_records": total_records,
        "total_checks": len(report_df),
        "checks_passed": pass_count,
        "checks_warned": warn_count,
        "checks_failed": fail_count,
        "total_issues_found": int(total_issues),
        "overall_status": overall
    }]

    pd.DataFrame(summary).to_csv("~/data_quality_summary.csv", index=False)

    print("\n" + "="*60)
    print("  DATA QUALITY AUDIT — FINAL REPORT")
    print("="*60)
    print(f"  Timestamp:          {summary[0]['report_timestamp']}")
    print(f"  Total records:      {total_records}")
    print(f"  Total checks run:   {len(report_df)}")
    print(f"  Passed:             {pass_count}")
    print(f"  Warnings:           {warn_count}")
    print(f"  Failed:             {fail_count}")
    print(f"  Total issues found: {int(total_issues)}")
    print(f"  Overall status:     {overall}")
    print("="*60)
    print("\n  DETAIL:")
    for _, row in report_df.iterrows():
        print(f"  [{row['status']}] {row['check']} — {row['field']}: {row['detail']}")
    print("="*60)


def run_audit():
    df = load_data()
    total_records = len(df)
    all_results = []
    all_results.extend(check_completeness(df))
    all_results.extend(check_duplicates(df))
    all_results.extend(check_format_validity(df))
    all_results.extend(check_outliers(df))
    all_results.extend(check_consistency(df))
    generate_report(all_results, total_records)

run_audit()
