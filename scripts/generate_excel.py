"""
generate_excel.py
─────────────────────────────────────────────────────────────
WHY THIS EXISTS:
  Instead of using Snowflake GENERATOR to make fake data,
  we generate a real Excel file — just like a bank would export
  from their core banking system. This makes the demo realistic
  and impressive for judges.

WHAT IT CREATES:
  data/finance_source_data.xlsx with 4 sheets:
    - Customers   (2,000 rows)
    - Accounts    (5,000 rows)
    - Transactions (10,000 rows — keep it fast for demo)
    - Risk_Events  (500 rows)

HOW TO RUN:
  python scripts/generate_excel.py
"""

import pandas as pd
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)  # Fixed seed = same data every time you run it

# ── Helpers ───────────────────────────────────────────────────────
def rand_date(start_days_ago=1825, end_days_ago=0):
    """Random date between start and end days ago."""
    offset = random.randint(end_days_ago, start_days_ago)
    return (datetime.now() - timedelta(days=offset)).strftime("%Y-%m-%d")

def rand_datetime(start_mins_ago=525960, end_mins_ago=0):
    offset = random.randint(end_mins_ago, start_mins_ago)
    return (datetime.now() - timedelta(minutes=offset)).strftime("%Y-%m-%d %H:%M:%S")

def rand_ref():
    return "REF-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))

# ── Name pools ────────────────────────────────────────────────────
FIRST_NAMES = ["James","Olivia","Liam","Emma","Noah","Ava","William","Sophia",
               "Benjamin","Isabella","Lucas","Mia","Henry","Charlotte","Alexander",
               "Amelia","Michael","Harper","Ethan","Evelyn","Aiden","Luna",
               "Mason","Aria","Logan","Chloe","Jackson","Penelope","Sebastian","Layla"]

LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller",
               "Davis","Rodriguez","Martinez","Hernandez","Lopez","Wilson",
               "Anderson","Taylor","Thomas","Moore","Jackson","Martin","Lee",
               "Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez",
               "Lewis","Robinson","Walker"]

COUNTRIES   = ["United States","United Kingdom","Canada","Germany","Australia",
               "France","Singapore","UAE","India","Japan"]

CITIES      = ["New York","London","Toronto","Berlin","Sydney","Chicago",
               "Manchester","Vancouver","Paris","Singapore","Dubai","Mumbai"]

MERCHANTS   = ["Amazon","Walmart","Starbucks","Netflix","Uber","Apple Store",
               "Shell Gas","Delta Airlines","CVS Pharmacy","Bank Transfer",
               "Spotify","Airbnb","McDonald's","Target","Best Buy","Zara",
               "Home Depot","Whole Foods","PayPal Transfer","Venmo"]

CATEGORIES  = {"Amazon":"SHOPPING","Walmart":"SHOPPING","Starbucks":"FOOD",
               "Netflix":"ENTERTAINMENT","Uber":"TRAVEL","Apple Store":"ELECTRONICS",
               "Shell Gas":"FUEL","Delta Airlines":"TRAVEL","CVS Pharmacy":"HEALTH",
               "Bank Transfer":"TRANSFER","Spotify":"ENTERTAINMENT",
               "Airbnb":"TRAVEL","McDonald's":"FOOD","Target":"SHOPPING",
               "Best Buy":"ELECTRONICS","Zara":"SHOPPING","Home Depot":"HOME",
               "Whole Foods":"FOOD","PayPal Transfer":"TRANSFER","Venmo":"TRANSFER"}

# ─────────────────────────────────────────────────────────────────
# SHEET 1: CUSTOMERS (2,000 rows)
# ─────────────────────────────────────────────────────────────────
print("Generating Customers...")

customers = []
for i in range(1, 2001):
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    dob_year = random.randint(1955, 2000)
    dob = f"{dob_year}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
    customers.append({
        "customer_id":      f"CUST-{i:06d}",
        "first_name":       fn,
        "last_name":        ln,
        "email":            f"{fn.lower()}.{ln.lower()}{i}@financebank.com",
        "phone":            f"+1-{random.randint(200,999)}-{random.randint(1000,9999)}",
        "date_of_birth":    dob,
        "country":          random.choice(COUNTRIES),
        "city":             random.choice(CITIES),
        "credit_score":     random.randint(300, 850),
        "annual_income":    round(random.randint(25000, 350000) / 1000) * 1000,
        "customer_segment": random.choices(
                                ["RETAIL","PREMIUM","BUSINESS"],
                                weights=[60, 25, 15])[0],
        "kyc_status":       random.choices(
                                ["VERIFIED","PENDING","FAILED"],
                                weights=[80, 15, 5])[0],
        "created_at":       rand_datetime(start_mins_ago=525960),
    })

df_customers = pd.DataFrame(customers)
print(f"  ✓ {len(df_customers)} customers created")

# ─────────────────────────────────────────────────────────────────
# SHEET 2: ACCOUNTS (5,000 rows)
# ─────────────────────────────────────────────────────────────────
print("Generating Accounts...")

accounts = []
for i in range(1, 5001):
    cust_num    = random.randint(1, 2000)
    acct_type   = random.choices(
                    ["CHECKING","SAVINGS","CREDIT","LOAN"],
                    weights=[35, 35, 20, 10])[0]
    balance     = round(random.uniform(-500, 250000), 2)
    credit_lim  = round(random.uniform(1000, 50000), 2) if acct_type == "CREDIT" else None
    interest    = {"CHECKING":0.0,"SAVINGS":round(random.uniform(0.01,0.05),4),
                   "CREDIT":round(random.uniform(0.12,0.29),4),
                   "LOAN":round(random.uniform(0.03,0.08),4)}[acct_type]
    status      = random.choices(["ACTIVE","FROZEN","CLOSED"], weights=[85,10,5])[0]
    opened      = rand_date(start_days_ago=2000, end_days_ago=30)
    accounts.append({
        "account_id":    f"ACC-{i:08d}",
        "customer_id":   f"CUST-{cust_num:06d}",
        "account_type":  acct_type,
        "account_number":f"{random.randint(10000000,99999999)}{random.randint(1000,9999)}",
        "currency":      random.choices(["USD","EUR","GBP","CAD"],
                                        weights=[50,20,15,15])[0],
        "balance":       balance,
        "credit_limit":  credit_lim,
        "interest_rate": interest,
        "status":        status,
        "opened_date":   opened,
        "closed_date":   None,
        "created_at":    rand_datetime(),
    })

df_accounts = pd.DataFrame(accounts)
print(f"  ✓ {len(df_accounts)} accounts created")

# ─────────────────────────────────────────────────────────────────
# SHEET 3: TRANSACTIONS (10,000 rows)
# ─────────────────────────────────────────────────────────────────
print("Generating Transactions...")

account_ids = [a["account_id"] for a in accounts]
transactions = []
for i in range(1, 10001):
    merchant = random.choice(MERCHANTS)
    txn_type = random.choices(
                    ["DEPOSIT","WITHDRAWAL","TRANSFER","PAYMENT","FEE"],
                    weights=[20, 25, 20, 30, 5])[0]
    amount   = round(random.uniform(0.50, 15000.00), 2)
    status   = random.choices(
                    ["COMPLETED","PENDING","FAILED","REVERSED"],
                    weights=[88, 7, 4, 1])[0]
    transactions.append({
        "transaction_id":   f"TXN-{i:010d}",
        "account_id":       random.choice(account_ids),
        "transaction_type": txn_type,
        "amount":           amount,
        "currency":         random.choices(["USD","EUR","GBP","CAD"],
                                           weights=[50,20,15,15])[0],
        "merchant_name":    merchant,
        "merchant_category":CATEGORIES[merchant],
        "channel":          random.choices(["ONLINE","ATM","MOBILE","BRANCH"],
                                           weights=[40,20,30,10])[0],
        "status":           status,
        "reference_code":   rand_ref(),
        "description":      f"Transaction #{i} — {merchant}",
        "transaction_dt":   rand_datetime(start_mins_ago=43800),
        "created_at":       rand_datetime(start_mins_ago=100),
    })

df_transactions = pd.DataFrame(transactions)
print(f"  ✓ {len(df_transactions)} transactions created")

# ─────────────────────────────────────────────────────────────────
# SHEET 4: RISK_EVENTS (500 rows)
# ─────────────────────────────────────────────────────────────────
print("Generating Risk Events...")

txn_ids  = [t["transaction_id"] for t in transactions]
risk_events = []
for i in range(1, 501):
    severity = random.choices(["LOW","MEDIUM","HIGH","CRITICAL"],
                               weights=[35, 35, 20, 10])[0]
    event_type = random.choice([
        "FRAUD_ALERT","LARGE_TRANSACTION","UNUSUAL_LOCATION",
        "MULTIPLE_FAILED_ATTEMPTS","VELOCITY_BREACH","ACCOUNT_TAKEOVER_SIGNAL"
    ])
    risk_events.append({
        "event_id":      f"RISK-{i:08d}",
        "account_id":    random.choice(account_ids),
        "transaction_id":random.choice(txn_ids),
        "event_type":    event_type,
        "severity":      severity,
        "score":         round(random.uniform(10.0, 99.99), 2),
        "description":   f"Risk signal detected — {event_type.replace('_',' ').title()} — event #{i}",
        "flagged_by":    random.choices(["RULE_ENGINE","ML_MODEL","MANUAL"],
                                        weights=[45, 40, 15])[0],
        "resolved":      random.choices([True, False], weights=[45, 55])[0],
        "event_dt":      rand_datetime(start_mins_ago=43800),
    })

df_risk = pd.DataFrame(risk_events)
print(f"  ✓ {len(df_risk)} risk events created")

# ─────────────────────────────────────────────────────────────────
# WRITE TO EXCEL
# ─────────────────────────────────────────────────────────────────
output_path = Path(__file__).parent.parent / "data" / "finance_source_data.xlsx"
output_path.parent.mkdir(exist_ok=True)

print(f"\nWriting Excel file → {output_path}")

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    df_customers.to_excel(   writer, sheet_name="Customers",    index=False)
    df_accounts.to_excel(    writer, sheet_name="Accounts",     index=False)
    df_transactions.to_excel(writer, sheet_name="Transactions", index=False)
    df_risk.to_excel(        writer, sheet_name="Risk_Events",  index=False)

total = len(df_customers) + len(df_accounts) + len(df_transactions) + len(df_risk)
print(f"""
✅ Done!
   File   : {output_path}
   Sheets : Customers ({len(df_customers):,}) | Accounts ({len(df_accounts):,}) | Transactions ({len(df_transactions):,}) | Risk Events ({len(df_risk):,})
   Total  : {total:,} rows

Next step: python scripts/load_to_snowflake.py
""")
