"""
app_local.py
────────────────────────────────────────────────────────────────
LOCAL VERSION — runs on your computer with streamlit run
Use this for development and testing in VS Code.

HOW TO RUN:
  pip install streamlit snowflake-connector-python python-dotenv
  streamlit run streamlit/app_local.py

The .env file in your root folder provides the credentials.
────────────────────────────────────────────────────────────────
"""

import os
import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

st.set_page_config(
    page_title="Finance ETL Monitor (Local)",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Local Snowflake connection (replaces get_active_session) ──────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        role      = os.environ.get("SNOWFLAKE_ROLE",      "ACCOUNTADMIN"),
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "ETL_WH"),
        database  = os.environ.get("SNOWFLAKE_DATABASE",  "FINANCE_ETL_DEMO"),
    )

@st.cache_data(ttl=60)
def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        cur.close()
        return df
    except Exception as e:
        return pd.DataFrame()

def fmt(n, d=0):
    try:
        if d == 0: return f"{int(float(n)):,}"
        return f"{float(n):,.{d}f}"
    except: return "—"

# ── SIDEBAR ───────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ Finance ETL")
    st.markdown("**Command Center** — Local Dev Mode")
    st.divider()

    lookback = st.selectbox("Lookback window",
        ["Last 24 hours","Last 7 days","Last 30 days","All time"], index=1)
    days = {"Last 24 hours":1,"Last 7 days":7,"Last 30 days":30,"All time":3650}[lookback]

    st.divider()

    if st.button("⚡ Run Pipeline Now", type="primary", use_container_width=True):
        with st.spinner("Running agent..."):
            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute("CALL FINANCE_ETL_DEMO.AUDIT.SP_AUTONOMOUS_ETL_AGENT(3, NULL)")
                result = cur.fetchone()
                cur.close()
                st.success("Pipeline completed!")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"{e}")

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── TABS ──────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "⚡ Overview",
    "🏦 Business Intelligence",
    "🛡️ Fraud & Risk",
    "📋 Pipeline Monitor",
    "🗄️ Row Counts",
])

# ── TAB 1 — OVERVIEW ─────────────────────────────────────────────
with tab1:
    st.header("Pipeline Health Overview")

    try:
        kpi = run_query(f"""
            SELECT COUNT(*) AS runs,
                   SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS ok,
                   SUM(CASE WHEN status='FAILED'  THEN 1 ELSE 0 END) AS fail,
                   ROUND(AVG(duration_sec),1) AS avg_dur,
                   ROUND(SUM(CASE WHEN status='SUCCESS' THEN 1.0 ELSE 0 END)/NULLIF(COUNT(*),0)*100,1) AS pct
            FROM FINANCE_ETL_DEMO.AUDIT.ETL_RUN_LOG
            WHERE started_at >= DATEADD('day',-{days},CURRENT_TIMESTAMP())
              AND procedure_name = 'SP_AUTONOMOUS_ETL_AGENT'
        """)
        if not kpi.empty:
            kpi.columns = [c.upper() for c in kpi.columns]
            row = kpi.iloc[0]
            c1,c2,c3,c4,c5 = st.columns(5)
            with c1: st.metric("Total Runs",    fmt(row.get("RUNS",0)))
            with c2: st.metric("Successful",    fmt(row.get("OK",0)))
            with c3: st.metric("Failed",        fmt(row.get("FAIL",0)))
            with c4: st.metric("Success Rate",  f"{row.get('PCT',0) or 0}%")
            with c5: st.metric("Avg Duration",  f"{row.get('AVG_DUR',0) or 0}s")
    except Exception as e:
        st.error(f"KPI error: {e}")

    st.divider()

    col_l, col_r = st.columns([2,1])
    with col_l:
        st.subheader("Run Timeline")
        try:
            tl = run_query(f"""
                SELECT DATE_TRUNC('hour',started_at) AS hr,
                       SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS ok,
                       SUM(CASE WHEN status='FAILED'  THEN 1 ELSE 0 END) AS err
                FROM FINANCE_ETL_DEMO.AUDIT.ETL_RUN_LOG
                WHERE started_at >= DATEADD('day',-{days},CURRENT_TIMESTAMP())
                  AND procedure_name='SP_AUTONOMOUS_ETL_AGENT'
                GROUP BY 1 ORDER BY 1
            """)
            if not tl.empty:
                tl.columns = [c.upper() for c in tl.columns]
                st.bar_chart(tl.rename(columns={"HR":"Hour"}).set_index("Hour")[["OK","ERR"]],
                             color=["#00d4ff","#ff4444"], height=250)
        except Exception as e:
            st.error(str(e))

    with col_r:
        st.subheader("Last 10 Runs")
        try:
            lr = run_query("""
                SELECT TO_CHAR(started_at,'MM/DD HH24:MI') AS t,
                       status, ROUND(duration_sec,1) AS d
                FROM FINANCE_ETL_DEMO.AUDIT.ETL_RUN_LOG
                WHERE procedure_name='SP_AUTONOMOUS_ETL_AGENT'
                ORDER BY started_at DESC LIMIT 10
            """)
            if not lr.empty:
                lr.columns = [c.upper() for c in lr.columns]
                for _, r in lr.iterrows():
                    icon = "🟢" if r["STATUS"]=="SUCCESS" else "🔴"
                    st.markdown(f"{icon} `{r['T']}` — **{r['D']}s**")
        except Exception as e:
            st.error(str(e))

# ── TAB 2 — BUSINESS INTELLIGENCE ────────────────────────────────
with tab2:
    st.header("Business Intelligence")

    try:
        biz = run_query("""
            SELECT COUNT(DISTINCT c.customer_id) AS customers,
                   SUM(a.balance)                AS portfolio_value,
                   COUNT(DISTINCT a.account_id)  AS accounts
            FROM FINANCE_ETL_DEMO.STAGING.CUSTOMERS c
            LEFT JOIN FINANCE_ETL_DEMO.STAGING.ACCOUNTS a ON c.customer_id=a.customer_id
        """).iloc[0]
        biz.index = [i.upper() for i in biz.index]

        txn = run_query("""
            SELECT COUNT(*) AS total_txns,
                   ROUND(SUM(amount_usd),2) AS volume
            FROM FINANCE_ETL_DEMO.STAGING.TRANSACTIONS
        """).iloc[0]
        txn.index = [i.upper() for i in txn.index]

        c1,c2,c3,c4 = st.columns(4)
        with c1: st.metric("Portfolio Value", f"${float(biz.get('PORTFOLIO_VALUE',0) or 0)/1e6:.1f}M")
        with c2: st.metric("Customers",       fmt(biz.get("CUSTOMERS",0)))
        with c3: st.metric("Accounts",        fmt(biz.get("ACCOUNTS",0)))
        with c4: st.metric("Txn Volume",      f"${float(txn.get('VOLUME',0) or 0)/1e6:.1f}M")
    except Exception as e:
        st.error(str(e))

    st.divider()
    st.subheader("Transaction Volume by Merchant Category")
    try:
        txn_cat = run_query("""
            SELECT merchant_category,
                   SUM(total_amount_usd)   AS volume,
                   SUM(total_transactions) AS count
            FROM FINANCE_ETL_DEMO.ANALYTICS.DAILY_TXN_SUMMARY
            GROUP BY 1 ORDER BY 2 DESC
        """)
        if not txn_cat.empty:
            txn_cat.columns = [c.upper() for c in txn_cat.columns]
            st.bar_chart(txn_cat.set_index("MERCHANT_CATEGORY")["VOLUME"], height=300)
    except Exception as e:
        st.error(str(e))

# ── TAB 3 — FRAUD & RISK ─────────────────────────────────────────
with tab3:
    st.header("Fraud & Risk Intelligence")

    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Top High-Risk Customers")
        try:
            hr = run_query("""
                SELECT customer_id, full_name, credit_tier,
                       customer_risk_tier,
                       ROUND(total_txn_volume_usd,0) AS volume,
                       risk_event_count,
                       ROUND(risk_score_avg,1) AS score
                FROM FINANCE_ETL_DEMO.ANALYTICS.CUSTOMER_FINANCIAL_HEALTH
                WHERE customer_risk_tier='HIGH_RISK'
                ORDER BY risk_score_avg DESC LIMIT 10
            """)
            if not hr.empty:
                hr.columns = [c.upper() for c in hr.columns]
                st.dataframe(hr, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(str(e))

    with col_r:
        st.subheader("Risk Events by Type")
        try:
            re = run_query("""
                SELECT event_type, COUNT(*) AS total,
                       SUM(CASE WHEN severity='CRITICAL' THEN 1 ELSE 0 END) AS critical,
                       ROUND(AVG(score),1) AS avg_score
                FROM FINANCE_ETL_DEMO.STAGING.RISK_EVENTS
                GROUP BY 1 ORDER BY 2 DESC
            """)
            if not re.empty:
                re.columns = [c.upper() for c in re.columns]
                st.dataframe(re, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(str(e))

# ── TAB 4 — PIPELINE MONITOR ─────────────────────────────────────
with tab4:
    st.header("Pipeline Monitor")

    try:
        proc = run_query(f"""
            SELECT procedure_name, layer,
                   COUNT(*) AS runs,
                   SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS ok,
                   SUM(CASE WHEN status='FAILED'  THEN 1 ELSE 0 END) AS fail,
                   ROUND(AVG(duration_sec),2) AS avg_s,
                   SUM(COALESCE(rows_inserted,0)) AS rows,
                   ROUND(SUM(CASE WHEN status='SUCCESS' THEN 1.0 ELSE 0 END)/NULLIF(COUNT(*),0)*100,1) AS pct
            FROM FINANCE_ETL_DEMO.AUDIT.ETL_RUN_LOG
            WHERE started_at >= DATEADD('day',-{days},CURRENT_TIMESTAMP())
            GROUP BY 1,2 ORDER BY 2,1
        """)
        if not proc.empty:
            proc.columns = [c.upper() for c in proc.columns]
            st.dataframe(proc, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(str(e))

    st.divider()
    st.subheader("Data Quality Checks")
    try:
        dq = run_query(f"""
            SELECT table_name, check_name, check_type, passed,
                   failed_rows, total_rows
            FROM FINANCE_ETL_DEMO.AUDIT.DATA_QUALITY_LOG
            WHERE checked_at >= DATEADD('day',-{days},CURRENT_TIMESTAMP())
            ORDER BY checked_at DESC
        """)
        if not dq.empty:
            dq.columns = [c.upper() for c in dq.columns]
            st.dataframe(dq, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(str(e))

# ── TAB 5 — ROW COUNTS ───────────────────────────────────────────
with tab5:
    st.header("Row Counts — All Layers")

    try:
        counts = run_query("""
            SELECT 'RAW' AS L,'CUSTOMERS' AS T,COUNT(*) AS N FROM FINANCE_ETL_DEMO.RAW.CUSTOMERS         UNION ALL
            SELECT 'RAW','ACCOUNTS',COUNT(*)                 FROM FINANCE_ETL_DEMO.RAW.ACCOUNTS          UNION ALL
            SELECT 'RAW','TRANSACTIONS',COUNT(*)             FROM FINANCE_ETL_DEMO.RAW.TRANSACTIONS      UNION ALL
            SELECT 'RAW','RISK_EVENTS',COUNT(*)              FROM FINANCE_ETL_DEMO.RAW.RISK_EVENTS       UNION ALL
            SELECT 'STAGING','CUSTOMERS',COUNT(*)            FROM FINANCE_ETL_DEMO.STAGING.CUSTOMERS     UNION ALL
            SELECT 'STAGING','ACCOUNTS',COUNT(*)             FROM FINANCE_ETL_DEMO.STAGING.ACCOUNTS      UNION ALL
            SELECT 'STAGING','TRANSACTIONS',COUNT(*)         FROM FINANCE_ETL_DEMO.STAGING.TRANSACTIONS  UNION ALL
            SELECT 'STAGING','RISK_EVENTS',COUNT(*)          FROM FINANCE_ETL_DEMO.STAGING.RISK_EVENTS   UNION ALL
            SELECT 'ANALYTICS','DAILY_TXN_SUMMARY',COUNT(*)  FROM FINANCE_ETL_DEMO.ANALYTICS.DAILY_TXN_SUMMARY       UNION ALL
            SELECT 'ANALYTICS','CUSTOMER_FINANCIAL_HEALTH',COUNT(*) FROM FINANCE_ETL_DEMO.ANALYTICS.CUSTOMER_FINANCIAL_HEALTH UNION ALL
            SELECT 'ANALYTICS','RISK_DASHBOARD',COUNT(*)     FROM FINANCE_ETL_DEMO.ANALYTICS.RISK_DASHBOARD
            ORDER BY 1,2
        """)
        if not counts.empty:
            counts.columns = [c.upper() for c in counts.columns]

            for layer, color in [("RAW","🥉"),("STAGING","🥈"),("ANALYTICS","🥇")]:
                subset = counts[counts["L"]==layer]
                if subset.empty: continue
                total = int(subset["N"].sum())
                st.markdown(f"**{color} {layer} — {total:,} total rows**")
                cols = st.columns(len(subset))
                for i, (_,r) in enumerate(subset.iterrows()):
                    with cols[i]:
                        st.metric(r["T"], f"{int(r['N']):,}")
                st.divider()
    except Exception as e:
        st.error(str(e))

# ── Footer ────────────────────────────────────────────────────────
st.divider()
st.caption("🔄 Local Dev Mode · Data refreshes every 60s · FINANCE_ETL_DEMO")