"""
load_to_snowflake.py
─────────────────────────────────────────────────────────────────
Reads data/finance_source_data.xlsx and loads each sheet
directly into Snowflake RAW tables using write_pandas.

No PUT command. No staging. No file path issues.
Works on all systems including paths with spaces.

HOW TO RUN:
  python scripts/load_to_snowflake.py
"""

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        role      = os.environ.get("SNOWFLAKE_ROLE",      "ACCOUNTADMIN"),
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "ETL_WH"),
        database  = os.environ.get("SNOWFLAKE_DATABASE",  "FINANCE_ETL_DEMO"),
        schema    = "RAW",
    )
    print("  ✓ Connected\n")
    return conn


# Map Excel sheet name → Snowflake RAW table name
SHEET_TO_TABLE = {
    "Customers":   "CUSTOMERS",
    "Accounts":    "ACCOUNTS",
    "Transactions":"TRANSACTIONS",
    "Risk_Events": "RISK_EVENTS",
}

# Columns to load for each sheet (must match RAW table columns exactly)
SHEET_COLUMNS = {
    "Customers": [
        "customer_id", "first_name", "last_name", "email", "phone",
        "date_of_birth", "country", "city", "credit_score", "annual_income",
        "customer_segment", "kyc_status", "created_at"
    ],
    "Accounts": [
        "account_id", "customer_id", "account_type", "account_number",
        "currency", "balance", "credit_limit", "interest_rate", "status",
        "opened_date", "closed_date", "created_at"
    ],
    "Transactions": [
        "transaction_id", "account_id", "transaction_type", "amount",
        "currency", "merchant_name", "merchant_category", "channel",
        "status", "reference_code", "description", "transaction_dt", "created_at"
    ],
    "Risk_Events": [
        "event_id", "account_id", "transaction_id", "event_type",
        "severity", "score", "description", "flagged_by", "resolved", "event_dt"
    ],
}


def main():
    excel_path = Path(__file__).parent.parent / "data" / "finance_source_data.xlsx"

    if not excel_path.exists():
        print("Excel file not found. Run this first:")
        print("  python scripts/generate_excel.py")
        return

    conn = get_connection()
    total_loaded = 0

    xl = pd.ExcelFile(excel_path)

    for sheet_name, table_name in SHEET_TO_TABLE.items():
        print(f"Loading sheet: {sheet_name} → RAW.{table_name}")

        # Read the sheet
        df = xl.parse(sheet_name)
        print(f"  Read {len(df):,} rows from Excel")

        # Keep only the columns that exist in the RAW table
        cols = SHEET_COLUMNS[sheet_name]
        df = df[[c for c in cols if c in df.columns]]

        # Rename columns to UPPERCASE — Snowflake needs uppercase column names
        df.columns = [c.upper() for c in df.columns]

        # Fix boolean column for Risk_Events
        if "RESOLVED" in df.columns:
            df["RESOLVED"] = df["RESOLVED"].astype(bool)

        # Convert date/datetime columns to strings to avoid type issues
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str).replace("nan", None)

        # Truncate the table first so we don't get duplicates on re-run
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE FINANCE_ETL_DEMO.RAW.{table_name}")
        cur.close()

        # Write directly to Snowflake using write_pandas
        success, n_chunks, n_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            database="FINANCE_ETL_DEMO",
            schema="RAW",
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=False,
        )

        if success:
            print(f"  ✓ Loaded {n_rows:,} rows into RAW.{table_name}\n")
            total_loaded += n_rows
        else:
            print(f"  ⚠ Failed to load RAW.{table_name}\n")

    # Verify final row counts
    print("─" * 50)
    print("Verifying row counts in Snowflake RAW layer:")
    cur = conn.cursor()
    for table_name in SHEET_TO_TABLE.values():
        cur.execute(f"SELECT COUNT(*) FROM FINANCE_ETL_DEMO.RAW.{table_name}")
        count = cur.fetchone()[0]
        print(f"  RAW.{table_name:<20} → {count:>8,} rows")
    cur.close()

    print(f"\n✅ Done! {total_loaded:,} total rows loaded into Snowflake.")
    print("\nNext step — run in Snowsight:")
    print("  CALL FINANCE_ETL_DEMO.AUDIT.SP_AUTONOMOUS_ETL_AGENT(3, NULL);")

    conn.close()


if __name__ == "__main__":
    main()