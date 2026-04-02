import pandas as pd
from datetime import datetime

def extract():
    print("[ EXTRACT ] Pulling raw campaign data...")
    raw_data = {
        "customer_id":  [1001,1002,1003,1004,1005,1006,1007,1002,1008,1009,1010,None],
        "first_name":   ["James","Tanya","Marcus","Lisa","Derek","Sandra",None,"Tanya","Kevin","Priya","Omar","Beth"],
        "last_name":    ["Holloway","Reed","Sullivan","Chang","Webb","Morton","Brooks",None,"Nash","Patel","Diaz","Ford"],
        "email":        ["j.holloway@email.com","tanya.reed@email.com","marcus.s@email.com","lisa.chang@email.com","derek.webb@email.com","not-an-email","brooks@email.com","tanya.reed@email.com","kevin.nash@email.com","priya.p@email.com","omar.d@email.com","beth.ford@email.com"],
        "zip_code":     ["72015","71101","72034","90210","71105","ABCDE","72015","71101","72032","10001",None,"72019"],
        "segment":      ["Prospect","Active","Lapsed","Active","prospect","Active","Lapsed","Active","PROSPECT","Active","Lapsed","Active"],
        "campaign_id":  ["C001","C002","C001","C003","C002","C001","C003","C002","C001","C003","C002",None],
        "response_flag":[1,0,1,1,0,1,0,0,1,1,0,1],
        "record_date":  ["2025-01-15","2025-01-15","2025-01-16","2025-01-16","2025-01-17","2025-01-17","2025-01-18","2025-01-15","2025-01-19","2025-01-19","2025-01-20","2025-01-20"]
    }
    df = pd.DataFrame(raw_data)
    print(f"    Raw records pulled: {len(df)}")
    return df

def transform(df):
    print("\n[ TRANSFORM ] Cleaning and standardizing data...")
    issues = []
    dupes = df.duplicated().sum()
    df = df.drop_duplicates()
    if dupes > 0:
        issues.append(f"Removed {dupes} exact duplicate row(s)")
        print(f"    Duplicates removed: {dupes}")
    id_dupes = df.duplicated(subset="customer_id").sum()
    df = df.drop_duplicates(subset="customer_id", keep="first")
    if id_dupes > 0:
        issues.append(f"Removed {id_dupes} duplicate customer_id(s)")
        print(f"    Duplicate customer IDs removed: {id_dupes}")
    null_ids = df["customer_id"].isnull().sum()
    df = df.dropna(subset=["customer_id"])
    if null_ids > 0:
        issues.append(f"Dropped {null_ids} record(s) with null customer_id")
        print(f"    Records dropped (null customer_id): {null_ids}")
    df["segment"] = df["segment"].str.strip().str.title()
    print(f"    Segment values standardized: {df['segment'].unique().tolist()}")
    email_mask = df["email"].str.contains(r"^[\w\.\+\-]+@[\w\-]+\.\w+$", na=False)
    invalid_emails = (~email_mask).sum()
    df["email_valid"] = email_mask
    if invalid_emails > 0:
        issues.append(f"Flagged {invalid_emails} record(s) with invalid email")
        print(f"    Invalid emails flagged: {invalid_emails}")
    zip_mask = df["zip_code"].str.match(r"^\d{5}$", na=False)
    invalid_zips = (~zip_mask).sum()
    df["zip_valid"] = zip_mask
    if invalid_zips > 0:
        issues.append(f"Flagged {invalid_zips} record(s) with invalid zip code")
        print(f"    Invalid zip codes flagged: {invalid_zips}")
    df["first_name"] = df["first_name"].fillna("Unknown")
    df["last_name"] = df["last_name"].fillna("Unknown")
    df["record_date"] = pd.to_datetime(df["record_date"])
    df["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"    Clean records remaining: {len(df)}")
    return df, issues

def load(df, issues):
    print("\n[ LOAD ] Writing outputs...")
    clean_df = df[df["email_valid"] & df["zip_valid"]].drop(columns=["email_valid","zip_valid"])
    flagged_df = df[~(df["email_valid"] & df["zip_valid"])]
    clean_df.to_csv("output_clean.csv", index=False)
    flagged_df.to_csv("output_flagged.csv", index=False)
    print(f"    Clean records written to output_clean.csv: {len(clean_df)}")
    print(f"    Flagged records written to output_flagged.csv: {len(flagged_df)}")
    return clean_df, flagged_df, issues

def log_run(original_count, clean_count, flagged_count, issues):
    print("\n[ AUDIT LOG ] Writing run summary...")
    log = {
        "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw_record_count": original_count,
        "clean_record_count": clean_count,
        "flagged_record_count": flagged_count,
        "pass_rate": f"{round((clean_count / original_count) * 100, 1)}%",
        "issues_found": len(issues),
        "issue_detail": " | ".join(issues) if issues else "None"
    }
    pd.DataFrame([log]).to_csv("etl_run_log.csv", index=False)
    print("\n" + "="*55)
    print("  ETL RUN COMPLETE — SUMMARY")
    print("="*55)
    for k, v in log.items():
        print(f"  {k:<25} {v}")
    print("="*55)

def run_pipeline():
    raw_df = extract()
    original_count = len(raw_df)
    transformed_df, issues = transform(raw_df)
    clean_df, flagged_df, issues = load(transformed_df, issues)
    log_run(original_count, len(clean_df), len(flagged_df), issues)

run_pipeline()
