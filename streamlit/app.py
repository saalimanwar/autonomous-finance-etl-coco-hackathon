"""
╔══════════════════════════════════════════════════════════════════════════╗
║   AUTONOMOUS FINANCE ETL AGENT — INTELLIGENCE PLATFORM                   ║
║   Snowflake Native · 100% Browser-Based · Powered by Cortex AI           ║
║   v3.0  |  Built with Snowflake CoCo (Cortex Code)                       ║
╚══════════════════════════════════════════════════════════════════════════╝

WHAT THIS SOLVES (Real-world problem):
  Financial institutions process millions of transactions daily across siloed
  systems. Data engineers babysit brittle pipelines. Fraud teams react too
  slowly. Business users can't self-serve insights without SQL expertise.
  Compliance teams lack audit trails.

  This platform eliminates all of that:
  ✦ Autonomous pipeline — runs every 15 min, self-heals on failure
  ✦ Bronze→Silver→Gold transformation — raw data becomes business intelligence
  ✦ Cortex AI — natural language anomaly explanations & data Q&A
  ✦ Full audit trail — every run logged with timing & row counts
  ✦ Zero infrastructure — 100% inside Snowflake, no external tools

HOW TO DEPLOY:
  1. Open Snowsight → Projects → Streamlit → + Streamlit App
  2. Set database = FINANCE_ETL_DEMO, schema = AUDIT
  3. Paste this file, click Run
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Finance Intelligence Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── GLOBAL CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Syne:wght@300;400;500;600;700&family=IBM+Plex+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }

.stApp { background: #060a10; }

section[data-testid="stSidebar"] {
    background: #0a0f18;
    border-right: 1px solid #141e2e;
}
section[data-testid="stSidebar"] .block-container { padding: 1.25rem 1rem; }
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span { color: #7a8fa8 !important; font-size: 12px !important; }

.block-container { padding: 1.5rem 2.5rem; max-width: 1500px; }

.stTabs [data-baseweb="tab-list"] {
    background: transparent;
    border-bottom: 1px solid #141e2e;
    gap: 0;
}
.stTabs [data-baseweb="tab"] {
    background: transparent;
    color: #3d5066;
    font-size: 12px;
    font-weight: 500;
    letter-spacing: 0.04em;
    padding: 10px 18px;
    border-bottom: 2px solid transparent;
}
.stTabs [aria-selected="true"] {
    color: #38bdf8 !important;
    border-bottom-color: #38bdf8 !important;
    background: transparent !important;
}
.stTabs [data-baseweb="tab-panel"] { padding-top: 2rem; }

[data-testid="metric-container"] {
    background: #0a0f18;
    border: 1px solid #141e2e;
    border-radius: 10px;
    padding: 18px 20px;
    transition: border-color 0.25s;
}
[data-testid="metric-container"]:hover { border-color: #38bdf833; }
[data-testid="metric-container"] label {
    color: #3d5066 !important;
    font-size: 10px !important;
    font-weight: 600 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #e2eaf4 !important;
    font-size: 26px !important;
    font-weight: 300 !important;
    font-family: 'IBM Plex Mono', monospace !important;
}

.stDataFrame { background: #0a0f18; border: 1px solid #141e2e; border-radius: 8px; }

.stButton > button {
    background: transparent;
    border: 1px solid #1e2e42;
    color: #7a8fa8;
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 12px;
    font-weight: 500;
    letter-spacing: 0.03em;
    border-radius: 7px;
    transition: all 0.2s;
    width: 100%;
}
.stButton > button:hover { background: #141e2e; border-color: #2a3e56; color: #e2eaf4; }
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #0c4a6e 0%, #0369a1 100%);
    border: 1px solid #38bdf830;
    color: #38bdf8;
    font-weight: 500;
}
.stButton > button[kind="primary"]:hover {
    background: linear-gradient(135deg, #0369a1 0%, #0284c7 100%);
    border-color: #38bdf855;
    color: #fff;
}
.stSelectbox > div > div {
    background: #0a0f18 !important;
    border: 1px solid #141e2e !important;
    color: #7a8fa8 !important;
    font-size: 13px !important;
    border-radius: 7px !important;
}
.stTextInput > div > div > input, .stTextArea textarea {
    background: #0a0f18 !important;
    border: 1px solid #141e2e !important;
    color: #c8d8ec !important;
    font-family: 'IBM Plex Sans', sans-serif !important;
    font-size: 13px !important;
    border-radius: 7px !important;
}
.stTextInput > div > div > input:focus, .stTextArea textarea:focus {
    border-color: #38bdf855 !important;
    box-shadow: 0 0 0 1px #38bdf820 !important;
}
.stNumberInput > div > div > input {
    background: #0a0f18 !important;
    border: 1px solid #141e2e !important;
    color: #7a8fa8 !important;
    font-size: 13px !important;
    border-radius: 7px !important;
}
hr { border-color: #141e2e !important; margin: 1.75rem 0 !important; }
.stSpinner > div { border-color: #38bdf8 transparent transparent !important; }
.stSuccess {
    background: #071a0e !important;
    border: 1px solid #16a34a33 !important;
    color: #4ade80 !important;
    border-radius: 8px !important;
}
.stError {
    background: #1a0707 !important;
    border: 1px solid #dc262633 !important;
    border-radius: 8px !important;
}
.stInfo {
    background: #071222 !important;
    border: 1px solid #38bdf822 !important;
    border-radius: 8px !important;
}
.stWarning {
    background: #1a1007 !important;
    border: 1px solid #f5971533 !important;
    border-radius: 8px !important;
}
</style>
""", unsafe_allow_html=True)

# ── Session & helpers ──────────────────────────────────────────────────────
session = get_active_session()

@st.cache_data(ttl=60)
def q(sql):
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        return pd.DataFrame()

def qr(sql):
    """Non-cached query for live/write operations."""
    return session.sql(sql).to_pandas()

def fmt(n, d=0, prefix=""):
    try:
        if d == 0: return f"{prefix}{int(float(n)):,}"
        return f"{prefix}{float(n):,.{d}f}"
    except: return "—"

def card(label, value, sub="", accent="#38bdf8"):
    return f"""
    <div style="background:#0a0f18;border:1px solid #141e2e;border-top:2px solid {accent}44;
                border-radius:10px;padding:18px 20px">
        <div style="font-size:10px;color:#3d5066;font-weight:600;letter-spacing:0.1em;
                    text-transform:uppercase;margin-bottom:8px">{label}</div>
        <div style="font-size:26px;font-weight:300;color:{accent};
                    font-family:'IBM Plex Mono',monospace;line-height:1">{value}</div>
        {f'<div style="font-size:11px;color:#3d5066;margin-top:6px;font-family:IBM Plex Mono,monospace">{sub}</div>' if sub else ''}
    </div>"""

def kv_row(label, value, value_color="#c8d8ec"):
    return f"""
    <div style="display:flex;justify-content:space-between;align-items:center;
                padding:8px 0;border-bottom:1px solid #141e2e">
        <span style="font-size:12px;color:#3d5066">{label}</span>
        <span style="font-size:12px;font-family:'IBM Plex Mono',monospace;
                     color:{value_color}">{value}</span>
    </div>"""

def section(title, sub=""):
    sub_html = f'<div style="font-size:10px;color:#3d5066;font-weight:600;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:6px">{sub}</div>' if sub else ""
    st.markdown(f"""
    <div style="margin-bottom:1.5rem">
        {sub_html}
        <h3 style="font-family:'Syne',sans-serif;color:#e2eaf4;font-size:20px;font-weight:500;
                   margin:0;letter-spacing:-0.02em">{title}</h3>
    </div>""", unsafe_allow_html=True)

def info_box(msg, color="#38bdf8", bg="#071222", icon="ℹ"):
    st.markdown(f"""
    <div style="background:{bg};border:1px solid {color}22;border-left:3px solid {color};
                border-radius:8px;padding:12px 16px;margin-bottom:1rem;
                font-size:13px;color:#7a8fa8;line-height:1.6">
        <span style="color:{color};margin-right:8px">{icon}</span>{msg}
    </div>""", unsafe_allow_html=True)

def table_html(headers, rows_data, col_widths=None):
    """Render a custom dark table."""
    width_str = f"grid-template-columns:{col_widths};" if col_widths else "grid-template-columns:repeat(auto-fit,minmax(80px,1fr));"
    hdr = "".join(f'<span style="font-size:10px;color:#3d5066;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;padding:9px 14px">{h}</span>' for h in headers)
    body = ""
    for row in rows_data:
        body += f'<div style="{width_str}display:grid;border-bottom:1px solid #141e2e">'
        for cell in row:
            body += f'<div style="padding:9px 14px">{cell}</div>'
        body += "</div>"
    return f"""
    <div style="background:#0a0f18;border:1px solid #141e2e;border-radius:10px;overflow:hidden">
        <div style="{width_str}display:grid;border-bottom:2px solid #141e2e">{hdr}</div>
        {body}
    </div>"""

def mono(val, color="#7a8fa8", size="12px"):
    return f'<span style="font-family:IBM Plex Mono,monospace;font-size:{size};color:{color}">{val}</span>'

def tag(text, color="#38bdf8", bg="#071222"):
    return f'<span style="font-size:10px;font-weight:600;letter-spacing:0.06em;background:{bg};color:{color};padding:2px 8px;border-radius:4px;border:1px solid {color}22">{text}</span>'

def status_dot(s):
    c = {"SUCCESS":"#4ade80","FAILED":"#f87171","SKIPPED":"#fbbf24"}.get(s,"#3d5066")
    return f'<span style="display:inline-flex;align-items:center;gap:6px"><span style="width:5px;height:5px;border-radius:50%;background:{c};flex-shrink:0;display:inline-block"></span><span style="font-size:12px;color:{c};font-weight:500">{s}</span></span>'

# ── SIDEBAR ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="margin-bottom:1.25rem">
        <div style="font-size:9px;color:#3d5066;font-weight:700;letter-spacing:0.18em;
                    text-transform:uppercase;margin-bottom:8px">Autonomous Finance ETL</div>
        <div style="font-family:'Syne',sans-serif;font-size:16px;color:#e2eaf4;
                    font-weight:600;line-height:1.25">Intelligence<br>Platform</div>
        <div style="margin-top:10px;display:flex;align-items:center;gap:6px">
            <div style="width:6px;height:6px;background:#4ade80;border-radius:50%"></div>
            <span style="font-size:10px;color:#4ade80;font-family:IBM Plex Mono,monospace;
                         font-weight:500">LIVE · FINANCE_ETL_DEMO</span>
        </div>
    </div>
    <div style="border-top:1px solid #141e2e;margin-bottom:1.25rem"></div>
    """, unsafe_allow_html=True)

    lookback = st.selectbox("Lookback window",
        ["Last 24 hours","Last 7 days","Last 30 days","All time"], index=1)
    days = {"Last 24 hours":1,"Last 7 days":7,"Last 30 days":30,"All time":3650}[lookback]

    st.markdown("<div style='border-top:1px solid #141e2e;margin:1rem 0'></div>", unsafe_allow_html=True)
    st.markdown('<p style="font-size:10px;color:#3d5066;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:10px">Pipeline Control</p>', unsafe_allow_html=True)

    if st.button("⚡  Run Pipeline Now", type="primary", use_container_width=True):
        with st.spinner("Agent orchestrating pipeline…"):
            try:
                result = session.sql("CALL FINANCE_ETL_DEMO.AUDIT.SP_AUTONOMOUS_ETL_AGENT(3, NULL)").collect()
                st.success("Pipeline completed successfully")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"{e}")

    if st.button("🔄  Refresh Data", use_container_width=True):
        st.cache_data.clear(); st.rerun()

    st.markdown("<div style='border-top:1px solid #141e2e;margin:1rem 0'></div>", unsafe_allow_html=True)
    st.markdown('<p style="font-size:10px;color:#3d5066;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:10px">Scheduled Task</p>', unsafe_allow_html=True)

    try:
        td = q("SHOW TASKS LIKE 'TASK_ETL_ORCHESTRATOR' IN SCHEMA FINANCE_ETL_DEMO.AUDIT")
        if not td.empty:
            state = td.iloc[0].get("state","UNKNOWN")
            sched = td.iloc[0].get("schedule","—")
            ok = state == "started"
            sc, st_txt = ("#4ade80","ACTIVE") if ok else ("#f87171","SUSPENDED")
            st.markdown(f"""
            <div style="background:#0a0f18;border:1px solid #141e2e;border-radius:8px;padding:12px;margin-bottom:10px">
                <div style="display:flex;justify-content:space-between;margin-bottom:5px">
                    <span style="font-size:10px;color:#3d5066;text-transform:uppercase;letter-spacing:0.08em">Status</span>
                    <span style="font-size:11px;color:{sc};font-weight:600;font-family:IBM Plex Mono,monospace">{st_txt}</span>
                </div>
                <div style="display:flex;justify-content:space-between">
                    <span style="font-size:10px;color:#3d5066;text-transform:uppercase;letter-spacing:0.08em">Schedule</span>
                    <span style="font-size:11px;color:#7a8fa8;font-family:IBM Plex Mono,monospace">{sched}</span>
                </div>
            </div>""", unsafe_allow_html=True)
            c1,c2 = st.columns(2)
            with c1:
                if st.button("⏸ Suspend", use_container_width=True):
                    session.sql("ALTER TASK FINANCE_ETL_DEMO.AUDIT.TASK_ETL_ORCHESTRATOR SUSPEND").collect()
                    st.cache_data.clear(); st.rerun()
            with c2:
                if st.button("▶ Resume", use_container_width=True):
                    session.sql("ALTER TASK FINANCE_ETL_DEMO.AUDIT.TASK_ETL_ORCHESTRATOR RESUME").collect()
                    st.cache_data.clear(); st.rerun()
    except Exception:
        st.warning("Task info unavailable")

    st.markdown("""
    <div style='border-top:1px solid #141e2e;margin:1.5rem 0 1rem'></div>
    <p style='color:#141e2e;font-size:10px;text-align:center;font-family:IBM Plex Mono,monospace'>v3.0 · CoCo Hackathon Build</p>
    """, unsafe_allow_html=True)


# ── HEALTH BANNER ──────────────────────────────────────────────────────────
try:
    brow = q("""SELECT
        SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS fail,
        ROUND(SUM(CASE WHEN status='SUCCESS' THEN 1.0 ELSE 0 END)/NULLIF(COUNT(*),0)*100,1) AS pct
        FROM FINANCE_ETL_DEMO.AUDIT.ETL_RUN_LOG
        WHERE started_at >= DATEADD('day',-1,CURRENT_TIMESTAMP())""").iloc[0]
    fail_n = int(brow.get("FAIL",0) or 0)
    pct = float(brow.get("PCT",100) or 100)
    if fail_n > 0:
        st.markdown(f"""
        <div style="background:linear-gradient(90deg,#1a0707,#0f0404);border:1px solid #dc262633;
                    border-radius:9px;padding:11px 18px;margin-bottom:1.5rem;
                    display:flex;align-items:center;gap:12px">
            <span style="font-size:15px">⚠️</span>
            <span style="color:#fca5a5;font-size:13px;font-weight:500">
                {fail_n} pipeline failure(s) in the last 24 hours · success rate {pct}%</span>
            <span style="color:#f87171;font-size:10px;font-family:IBM Plex Mono,monospace;
                         margin-left:auto;font-weight:600;letter-spacing:0.08em">CHECK PIPELINE TAB</span>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="background:linear-gradient(90deg,#071a0e,#040f08);border:1px solid #16a34a22;
                    border-radius:9px;padding:11px 18px;margin-bottom:1.5rem;
                    display:flex;align-items:center;gap:12px">
            <span style="font-size:15px">✅</span>
            <span style="color:#86efac;font-size:13px;font-weight:500">All systems nominal · 100% success rate last 24h</span>
            <span style="color:#4ade8066;font-size:10px;font-family:IBM Plex Mono,monospace;
                         margin-left:auto;font-weight:600;letter-spacing:0.08em">PIPELINE HEALTHY</span>
        </div>""", unsafe_allow_html=True)
except Exception:
    pass


# ── TABS ───────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "⚡ Command Center",
    "🏦 Business Intelligence",
    "🛡️ Fraud & Risk",
    "👤 Customer 360",
    "🤖 Cortex AI Analyst",
    "📋 Pipeline Monitor",
    "🏗️ SQL Generator",
    "💬 CoCo Prompts",
])


# ══════════════════════════════════════════════════════════════════════════
# TAB 1 — COMMAND CENTER
# ══════════════════════════════════════════════════════════════════════════
with tab1:

    # ── Hero statement ────────────────────────────────────────────────────
    st.markdown("""
    <div style="margin-bottom:2rem">
        <div style="font-size:10px;color:#3d5066;font-weight:700;letter-spacing:0.18em;
                    text-transform:uppercase;margin-bottom:10px">What problem we solve</div>
        <p style="font-family:'Syne',sans-serif;font-size:24px;font-weight:400;color:#c8d8ec;
                  line-height:1.5;max-width:860px;margin:0">
            Financial institutions process millions of transactions daily —
            but raw data sitting in silos doesn't pay the bills.
            <span style="color:#38bdf8">This platform transforms it automatically.</span>
        </p>
        <div style="display:flex;gap:20px;margin-top:16px;flex-wrap:wrap">
            <span style="font-size:12px;color:#3d5066">✦ Zero manual intervention</span>
            <span style="font-size:12px;color:#3d5066">✦ Self-healing on failure</span>
            <span style="font-size:12px;color:#3d5066">✦ AI-powered anomaly detection</span>
            <span style="font-size:12px;color:#3d5066">✦ Plain-English data Q&A</span>
            <span style="font-size:12px;color:#3d5066">✦ 100% inside Snowflake</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Business KPIs ─────────────────────────────────────────────────────
    section("Live Business Metrics", "Portfolio Overview")
    try:
        biz = q("""SELECT
            COUNT(DISTINCT c.customer_id)                          AS customers,
            SUM(a.balance)                                         AS portfolio_value,
            COUNT(DISTINCT a.account_id)                           AS accounts,
            COUNT(DISTINCT CASE WHEN a.is_active THEN a.account_id END) AS active_accts,
            COUNT(DISTINCT CASE WHEN c.kyc_status='VERIFIED' THEN c.customer_id END) AS kyc_verified,
            COUNT(DISTINCT CASE WHEN ch.customer_risk_tier='HIGH_RISK' THEN ch.customer_id END) AS high_risk
        FROM FINANCE_ETL_DEMO.STAGING.CUSTOMERS c
        LEFT JOIN FINANCE_ETL_DEMO.STAGING.ACCOUNTS a ON c.customer_id=a.customer_id
        LEFT JOIN FINANCE_ETL_DEMO.ANALYTICS.CUSTOMER_FINANCIAL_HEALTH ch ON c.customer_id=ch.customer_id""").iloc[0]

        txn_biz = q("""SELECT
            COUNT(*) AS total_txns,
            ROUND(SUM(amount_usd),2) AS total_volume,
            ROUND(SUM(CASE WHEN is_successful THEN 1.0 ELSE 0 END)/NULLIF(COUNT(*),0)*100,1) AS success_pct,
            COUNT(DISTINCT CASE WHEN is_large_txn THEN transaction_id END) AS large_txns
        FROM FINANCE_ETL_DEMO.STAGING.TRANSACTIONS""").iloc[0]

        cols = st.columns(4)
        metrics = [
            ("Portfolio Value", f"${float(biz.get('PORTFOLIO_VALUE',0) or 0)/1e6:.1f}M", "total balance across all accounts", "#38bdf8"),
            ("Total Customers", fmt(biz.get("CUSTOMERS",0)), f"{fmt(biz.get('KYC_VERIFIED',0))} KYC verified", "#4ade80"),
            ("Transactions", fmt(txn_biz.get("TOTAL_TXNS",0)), f"${float(txn_biz.get('TOTAL_VOLUME',0) or 0)/1e6:.1f}M volume", "#a78bfa"),
            ("High-Risk Accounts", fmt(biz.get("HIGH_RISK",0)), f"of {fmt(biz.get('ACCOUNTS',0))} total accounts", "#f87171"),
        ]
        for i, (label, value, sub, accent) in enumerate(metrics):
            with cols[i]:
                st.markdown(card(label, value, sub, accent), unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Business metrics error: {e}")

    st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)

    # ── Pipeline architecture visual ──────────────────────────────────────
    section("Pipeline Architecture", "Bronze → Silver → Gold Data Flow")
    try:
        counts_raw = q("""
        SELECT 'RAW' AS L,'CUSTOMERS' AS T, COUNT(*) AS N FROM FINANCE_ETL_DEMO.RAW.CUSTOMERS UNION ALL
        SELECT 'RAW','ACCOUNTS', COUNT(*) FROM FINANCE_ETL_DEMO.RAW.ACCOUNTS UNION ALL
        SELECT 'RAW','TRANSACTIONS', COUNT(*) FROM FINANCE_ETL_DEMO.RAW.TRANSACTIONS UNION ALL
        SELECT 'RAW','RISK_EVENTS', COUNT(*) FROM FINANCE_ETL_DEMO.RAW.RISK_EVENTS UNION ALL
        SELECT 'STAGING','CUSTOMERS', COUNT(*) FROM FINANCE_ETL_DEMO.STAGING.CUSTOMERS UNION ALL
        SELECT 'STAGING','ACCOUNTS', COUNT(*) FROM FINANCE_ETL_DEMO.STAGING.ACCOUNTS UNION ALL
        SELECT 'STAGING','TRANSACTIONS', COUNT(*) FROM FINANCE_ETL_DEMO.STAGING.TRANSACTIONS UNION ALL
        SELECT 'STAGING','RISK_EVENTS', COUNT(*) FROM FINANCE_ETL_DEMO.STAGING.RISK_EVENTS UNION ALL
        SELECT 'ANALYTICS','DAILY_TXN_SUMMARY', COUNT(*) FROM FINANCE_ETL_DEMO.ANALYTICS.DAILY_TXN_SUMMARY UNION ALL
        SELECT 'ANALYTICS','CUSTOMER_FINANCIAL_HEALTH', COUNT(*) FROM FINANCE_ETL_DEMO.ANALYTICS.CUSTOMER_FINANCIAL_HEALTH UNION ALL
        SELECT 'ANALYTICS','RISK_DASHBOARD', COUNT(*) FROM FINANCE_ETL_DEMO.ANALYTICS.RISK_DASHBOARD""")
        counts_raw.columns = [c.upper() for c in counts_raw.columns]

        def layer_total(layer):
            subset = counts_raw[counts_raw["L"]==layer]
            return int(subset["N"].sum()) if not subset.empty else 0

        raw_total = layer_total("RAW")
        stg_total = layer_total("STAGING")
        ana_total = layer_total("ANALYTICS")

        def layer_block(name, emoji, desc, color, total, tables_data):
            tables_html = "".join(f"""
            <div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #141e2e">
                <span style="font-size:11px;color:#7a8fa8">{t}</span>
                <span style="font-size:11px;font-family:IBM Plex Mono,monospace;color:{color}">{n:,}</span>
            </div>""" for t,n in tables_data)
            return f"""
            <div style="background:#0a0f18;border:1px solid #141e2e;border-top:3px solid {color};
                        border-radius:10px;padding:18px">
                <div style="font-size:18px;margin-bottom:6px">{emoji}</div>
                <div style="font-family:'Syne',sans-serif;font-size:15px;font-weight:600;color:#e2eaf4;margin-bottom:4px">{name}</div>
                <div style="font-size:11px;color:#3d5066;margin-bottom:12px">{desc}</div>
                <div style="font-family:IBM Plex Mono,monospace;font-size:22px;font-weight:300;color:{color};margin-bottom:12px">{total:,}</div>
                <div style="font-size:10px;color:#3d5066;letter-spacing:0.08em;text-transform:uppercase;font-weight:600;margin-bottom:8px">Tables</div>
                {tables_html}
            </div>"""

        raw_tables = [(r["T"], int(r["N"])) for _, r in counts_raw[counts_raw["L"]=="RAW"].iterrows()]
        stg_tables = [(r["T"], int(r["N"])) for _, r in counts_raw[counts_raw["L"]=="STAGING"].iterrows()]
        ana_tables = [(r["T"], int(r["N"])) for _, r in counts_raw[counts_raw["L"]=="ANALYTICS"].iterrows()]

        c1, cx, c2, cy, c3 = st.columns([3, 0.4, 3, 0.4, 3])
        with c1:
            st.markdown(layer_block("Bronze · RAW","🥉","Direct ingest from source systems — no transformation","#f59e0b", raw_total, raw_tables), unsafe_allow_html=True)
        with cx:
            st.markdown("""
            <div style="display:flex;align-items:center;justify-content:center;height:100%;
                        padding-top:120px;color:#3d5066;font-size:22px">→</div>""", unsafe_allow_html=True)
        with c2:
            st.markdown(layer_block("Silver · STAGING","🥈","Cleaned, enriched, deduped — enriched with derived fields","#38bdf8", stg_total, stg_tables), unsafe_allow_html=True)
        with cy:
            st.markdown("""
            <div style="display:flex;align-items:center;justify-content:center;height:100%;
                        padding-top:120px;color:#3d5066;font-size:22px">→</div>""", unsafe_allow_html=True)
        with c3:
            st.markdown(layer_block("Gold · ANALYTICS","🥇","Business-ready aggregates — dashboard & ML-ready","#4ade80", ana_total, ana_tables), unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Architecture error: {e}")

    st.markdown("---")

    # ── Agent run performance ─────────────────────────────────────────────
    section("Recent Pipeline Runs", "Autonomous agent execution log")
    try:
        run_df = q(f"""
        SELECT TO_CHAR(started_at,'MM/DD HH24:MI') AS t, status,
               ROUND(duration_sec,1) AS d, rows_inserted
        FROM FINANCE_ETL_DEMO.AUDIT.ETL_RUN_LOG
        WHERE procedure_name='SP_AUTONOMOUS_ETL_AGENT'
        ORDER BY started_at DESC LIMIT 10""")
        if not run_df.empty:
            run_df.columns = [c.upper() for c in run_df.columns]
            rows_html = ""
            for _, r in run_df.iterrows():
                s = str(r.get("STATUS",""))
                sc2 = {"SUCCESS":"#4ade80","FAILED":"#f87171"}.get(s,"#fbbf24")
                rows_html += f"""
                <div style="display:flex;justify-content:space-between;align-items:center;
                             padding:9px 16px;border-bottom:1px solid #141e2e">
                    <div style="display:flex;align-items:center;gap:10px">
                        <div style="width:5px;height:5px;border-radius:50%;background:{sc2}"></div>
                        <span style="font-family:IBM Plex Mono,monospace;font-size:11px;color:#7a8fa8">{r['T']}</span>
                    </div>
                    <span style="font-size:11px;color:{sc2};font-weight:500">{s}</span>
                    <span style="font-family:IBM Plex Mono,monospace;font-size:11px;color:#3d5066">{r['D']}s</span>
                </div>"""
            st.markdown(f'<div style="background:#0a0f18;border:1px solid #141e2e;border-radius:10px;overflow:hidden">{rows_html}</div>', unsafe_allow_html=True)
    except Exception as e:
        st.error(str(e))


# ══════════════════════════════════════════════════════════════════════════
# TAB 2 — BUSINESS INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════
with tab2:
    section("Business Intelligence", "Finance Analytics — What the data tells us")

    info_box("This is the Gold layer — the reason the pipeline exists. Raw transactional data transformed into actionable business metrics that finance, risk, and operations teams can act on.", "#38bdf8", "#071222", "💡")

    # ── Transaction volume overview ───────────────────────────────────────
    try:
        txn_vol = q("""
        SELECT merchant_category,
               SUM(total_amount_usd)          AS volume,
               SUM(total_transactions)         AS txn_count,
               ROUND(AVG(success_rate_pct),1)  AS success_pct,
               SUM(large_txn_count)            AS large_txns
        FROM FINANCE_ETL_DEMO.ANALYTICS.DAILY_TXN_SUMMARY
        GROUP BY 1 ORDER BY 2 DESC""")

        if not txn_vol.empty:
            txn_vol.columns = [c.upper() for c in txn_vol.columns]

            col_l, col_r = st.columns([2,1], gap="large")
            with col_l:
                section("Transaction Volume by Category", "All time")
                st.bar_chart(txn_vol.set_index("MERCHANT_CATEGORY")["VOLUME"], height=260)
            with col_r:
                section("Category Breakdown", "Ranked by volume")
                rows = []
                for _, r in txn_vol.iterrows():
                    sp = float(r.get("SUCCESS_PCT",0) or 0)
                    sp_c = "#4ade80" if sp>=90 else "#fbbf24" if sp>=80 else "#f87171"
                    rows.append([
                        f'<span style="font-size:12px;color:#c8d8ec">{r["MERCHANT_CATEGORY"]}</span>',
                        mono(f'${float(r["VOLUME"])/1e6:.1f}M', "#38bdf8"),
                        f'<span style="font-size:11px;color:{sp_c};font-family:IBM Plex Mono,monospace">{sp}%</span>',
                    ])
                st.markdown(table_html(["Category","Volume","Success"], rows, "2fr 1fr 0.8fr"), unsafe_allow_html=True)
    except Exception as e:
        st.error(str(e))

    st.markdown("---")

    # ── Transaction trend + channel breakdown ─────────────────────────────
    col_a, col_b = st.columns(2, gap="large")
    with col_a:
        section("Channel Mix", "How customers transact")
        try:
            ch_df = q("""
            SELECT channel,
                   SUM(total_transactions)  AS cnt,
                   SUM(total_amount_usd)    AS vol
            FROM FINANCE_ETL_DEMO.ANALYTICS.DAILY_TXN_SUMMARY
            GROUP BY 1 ORDER BY 2 DESC""")
            if not ch_df.empty:
                ch_df.columns = [c.upper() for c in ch_df.columns]
                total_ch = int(ch_df["CNT"].sum())
                rows = []
                for _, r in ch_df.iterrows():
                    pct = int(r["CNT"])/total_ch*100
                    rows.append([
                        f'<span style="font-size:12px;color:#c8d8ec">{r["CHANNEL"]}</span>',
                        f"""<div style="display:flex;align-items:center;gap:8px">
                              <div style="flex:1;height:4px;background:#141e2e;border-radius:2px">
                                <div style="width:{pct:.0f}%;height:100%;background:#38bdf8;border-radius:2px"></div>
                              </div>
                              <span style="font-family:IBM Plex Mono,monospace;font-size:11px;color:#7a8fa8;min-width:36px">{pct:.0f}%</span>
                            </div>""",
                        mono(fmt(r["CNT"]), "#a78bfa"),
                    ])
                st.markdown(table_html(["Channel","Share","Transactions"], rows, "1fr 2fr 1fr"), unsafe_allow_html=True)
        except Exception as e:
            st.error(str(e))

    with col_b:
        section("Transaction Type Mix", "Deposit, withdrawal, transfer breakdown")
        try:
            type_df = q("""
            SELECT transaction_type,
                   SUM(total_transactions)  AS cnt,
                   SUM(total_amount_usd)    AS vol,
                   ROUND(AVG(success_rate_pct),1) AS success_pct
            FROM FINANCE_ETL_DEMO.ANALYTICS.DAILY_TXN_SUMMARY
            GROUP BY 1 ORDER BY 2 DESC""")
            if not type_df.empty:
                type_df.columns = [c.upper() for c in type_df.columns]
                ttotal = int(type_df["CNT"].sum())
                rows = []
                for _, r in type_df.iterrows():
                    pct = int(r["CNT"])/ttotal*100
                    sp = float(r.get("SUCCESS_PCT",0) or 0)
                    sp_c = "#4ade80" if sp>=90 else "#fbbf24" if sp>=80 else "#f87171"
                    rows.append([
                        f'<span style="font-size:12px;color:#c8d8ec">{r["TRANSACTION_TYPE"]}</span>',
                        mono(f'{pct:.0f}%', "#a78bfa"),
                        f'<span style="font-size:11px;color:{sp_c};font-family:IBM Plex Mono,monospace">{sp}%</span>',
                    ])
                st.markdown(table_html(["Type","Share","Success Rate"], rows, "2fr 1fr 1.2fr"), unsafe_allow_html=True)
        except Exception as e:
            st.error(str(e))

    st.markdown("---")

    # ── Customer segment distribution ─────────────────────────────────────
    section("Customer Portfolio Health", "Segment × Risk tier breakdown")
    try:
        seg_df = q("""
        SELECT customer_segment, customer_risk_tier, COUNT(*) AS customers,
               ROUND(AVG(total_txn_volume_usd),0) AS avg_volume,
               ROUND(AVG(risk_score_avg),1) AS avg_risk_score
        FROM FINANCE_ETL_DEMO.ANALYTICS.CUSTOMER_FINANCIAL_HEALTH
        GROUP BY 1,2 ORDER BY 1,2""")
        if not seg_df.empty:
            seg_df.columns = [c.upper() for c in seg_df.columns]
            risk_colors = {"HIGH_RISK":"#f87171","MEDIUM_RISK":"#fbbf24","LOW_RISK":"#4ade80"}
            rows = []
            for _, r in seg_df.iterrows():
                rt = str(r.get("CUSTOMER_RISK_TIER",""))
                rc = risk_colors.get(rt,"#7a8fa8")
                rows.append([
                    f'<span style="font-size:12px;color:#c8d8ec;font-weight:500">{r.get("CUSTOMER_SEGMENT","")}</span>',
                    tag(rt, rc, rc+"22"),
                    mono(fmt(r.get("CUSTOMERS",0)),"#38bdf8"),
                    mono(f'${float(r.get("AVG_VOLUME",0) or 0)/1e3:.0f}K',"#a78bfa"),
                    f'<span style="font-size:12px;font-family:IBM Plex Mono,monospace;color:{rc}">{r.get("AVG_RISK_SCORE","—")}</span>',
                ])
            st.markdown(table_html(["Segment","Risk Tier","Customers","Avg Volume","Avg Risk Score"], rows, "1.5fr 1.5fr 1fr 1fr 1.2fr"), unsafe_allow_html=True)
    except Exception as e:
        st.error(str(e))

    st.markdown("---")

    # ── Income bracket analysis ───────────────────────────────────────────
    section("Customer Income & Credit Distribution", "Financial health of the portfolio")
    try:
        credit_df = q("""
        SELECT credit_tier,
               COUNT(*) AS customers,
               ROUND(AVG(total_txn_volume_usd),0) AS avg_txn_vol,
               COUNT(CASE WHEN customer_risk_tier='HIGH_RISK' THEN 1 END) AS high_risk_count
        FROM FINANCE_ETL_DEMO.ANALYTICS.CUSTOMER_FINANCIAL_HEALTH
        GROUP BY 1 ORDER BY 2 DESC""")
        inc_df = q("""
        SELECT income_bracket,
               COUNT(*) AS customers,
               ROUND(AVG(annual_income),0) AS avg_income,
               ROUND(AVG(credit_score),0) AS avg_credit
        FROM FINANCE_ETL_DEMO.STAGING.CUSTOMERS
        GROUP BY 1 ORDER BY 2 DESC""")

        col1, col2 = st.columns(2, gap="large")
        with col1:
            if not credit_df.empty:
                credit_df.columns = [c.upper() for c in credit_df.columns]
                credit_colors = {"EXCELLENT":"#4ade80","GOOD":"#38bdf8","FAIR":"#fbbf24","POOR":"#f87171"}
                rows = []
                for _, r in credit_df.iterrows():
                    ct = str(r.get("CREDIT_TIER",""))
                    cc = credit_colors.get(ct,"#7a8fa8")
                    rows.append([
                        tag(ct, cc, cc+"22"),
                        mono(fmt(r.get("CUSTOMERS",0)),"#38bdf8"),
                        mono(f'${float(r.get("AVG_TXN_VOL",0) or 0)/1e3:.0f}K',"#a78bfa"),
                        mono(fmt(r.get("HIGH_RISK_COUNT",0)),"#f87171"),
                    ])
                st.markdown(table_html(["Credit Tier","Customers","Avg Txn Vol","High Risk"], rows, "1.5fr 1fr 1fr 1fr"), unsafe_allow_html=True)
        with col2:
            if not inc_df.empty:
                inc_df.columns = [c.upper() for c in inc_df.columns]
                inc_colors = {"HIGH":"#4ade80","MEDIUM":"#38bdf8","LOW":"#fbbf24"}
                rows = []
                for _, r in inc_df.iterrows():
                    ib = str(r.get("INCOME_BRACKET",""))
                    ic = inc_colors.get(ib,"#7a8fa8")
                    rows.append([
                        tag(ib, ic, ic+"22"),
                        mono(fmt(r.get("CUSTOMERS",0)),"#38bdf8"),
                        mono(f'${float(r.get("AVG_INCOME",0) or 0)/1e3:.0f}K',"#a78bfa"),
                        mono(fmt(r.get("AVG_CREDIT",0)),"#4ade80"),
                    ])
                st.markdown(table_html(["Income Bracket","Customers","Avg Income","Avg Credit"], rows, "1.5fr 1fr 1fr 1fr"), unsafe_allow_html=True)
    except Exception as e:
        st.error(str(e))


# ══════════════════════════════════════════════════════════════════════════
# TAB 3 — FRAUD & RISK
# ══════════════════════════════════════════════════════════════════════════
with tab3:
    section("Fraud & Risk Intelligence", "Real-time risk monitoring across all accounts")

    # ── Risk KPIs ─────────────────────────────────────────────────────────
    try:
        rk = q("""SELECT
            COUNT(*) AS total_events,
            SUM(CASE WHEN severity='CRITICAL' THEN 1 ELSE 0 END) AS critical,
            SUM(CASE WHEN NOT resolved THEN 1 ELSE 0 END) AS unresolved,
            ROUND(AVG(score),1) AS avg_score,
            SUM(CASE WHEN event_type='FRAUD_ALERT' THEN 1 ELSE 0 END) AS fraud_alerts,
            SUM(CASE WHEN flagged_by='ML_MODEL' THEN 1 ELSE 0 END) AS ml_flagged
        FROM FINANCE_ETL_DEMO.STAGING.RISK_EVENTS""").iloc[0]

        cols = st.columns(4)
        for i, (lbl, val, sub, acc) in enumerate([
            ("Total Risk Events",  fmt(rk.get("TOTAL_EVENTS",0)),    f"{fmt(rk.get('UNRESOLVED',0))} unresolved", "#f87171"),
            ("Critical Events",    fmt(rk.get("CRITICAL",0)),         "highest severity tier",                     "#f87171"),
            ("Fraud Alerts",       fmt(rk.get("FRAUD_ALERTS",0)),     f"{fmt(rk.get('ML_FLAGGED',0))} ML-flagged", "#fbbf24"),
            ("Avg Risk Score",     str(rk.get("AVG_SCORE","—")),      "across all flagged accounts",               "#a78bfa"),
        ]):
            with cols[i]:
                st.markdown(card(lbl, val, sub, acc), unsafe_allow_html=True)
    except Exception as e:
        st.error(str(e))

    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

    # ── Risk event breakdown + unresolved ─────────────────────────────────
    col_l, col_r = st.columns([1,1], gap="large")

    with col_l:
        section("Events by Type & Severity", "Breakdown of all risk signals")
        try:
            et_df = q("""
            SELECT event_type,
                   COUNT(*) AS total,
                   SUM(CASE WHEN severity='CRITICAL' THEN 1 ELSE 0 END) AS critical,
                   SUM(CASE WHEN severity='HIGH' THEN 1 ELSE 0 END) AS high,
                   ROUND(AVG(score),1) AS avg_score,
                   SUM(CASE WHEN NOT resolved THEN 1 ELSE 0 END) AS unresolved
            FROM FINANCE_ETL_DEMO.STAGING.RISK_EVENTS
            GROUP BY 1 ORDER BY 2 DESC""")
            if not et_df.empty:
                et_df.columns = [c.upper() for c in et_df.columns]
                rows = []
                for _, r in et_df.iterrows():
                    total = int(r.get("TOTAL",0))
                    crit = int(r.get("CRITICAL",0))
                    crit_pct = crit/total*100 if total>0 else 0
                    rows.append([
                        f'<span style="font-size:11px;color:#c8d8ec">{r.get("EVENT_TYPE","")}</span>',
                        mono(fmt(r.get("TOTAL",0)),"#7a8fa8"),
                        f'<span style="font-size:11px;font-family:IBM Plex Mono,monospace;color:#f87171">{crit}</span>',
                        mono(str(r.get("AVG_SCORE","—")),"#a78bfa"),
                        mono(fmt(r.get("UNRESOLVED",0)),"#fbbf24"),
                    ])
                st.markdown(table_html(["Event Type","Total","Critical","Avg Score","Unresolved"], rows, "2.5fr 0.8fr 0.8fr 0.9fr 1fr"), unsafe_allow_html=True)
        except Exception as e:
            st.error(str(e))

    with col_r:
        section("Top 10 Highest-Risk Accounts", "Accounts requiring immediate attention")
        try:
            hr_df = q("""
            SELECT customer_id, full_name, credit_tier, customer_risk_tier,
                   risk_event_count, ROUND(risk_score_avg,1) AS score,
                   ROUND(total_txn_volume_usd,0) AS volume
            FROM FINANCE_ETL_DEMO.ANALYTICS.CUSTOMER_FINANCIAL_HEALTH
            WHERE customer_risk_tier='HIGH_RISK'
            ORDER BY risk_score_avg DESC LIMIT 10""")
            if not hr_df.empty:
                hr_df.columns = [c.upper() for c in hr_df.columns]
                rows = []
                for _, r in hr_df.iterrows():
                    sc_val = float(r.get("SCORE",0) or 0)
                    sc_c = "#f87171" if sc_val>=80 else "#fbbf24" if sc_val>=60 else "#7a8fa8"
                    rows.append([
                        f'<div><div style="font-size:12px;color:#c8d8ec">{r.get("FULL_NAME","")}</div><div style="font-size:10px;font-family:IBM Plex Mono,monospace;color:#3d5066">{r.get("CUSTOMER_ID","")}</div></div>',
                        tag(str(r.get("CREDIT_TIER","")), "#7a8fa8", "#141e2e"),
                        f'<span style="font-size:13px;font-weight:600;font-family:IBM Plex Mono,monospace;color:{sc_c}">{sc_val}</span>',
                        mono(fmt(r.get("RISK_EVENT_COUNT",0)),"#fbbf24"),
                    ])
                st.markdown(table_html(["Customer","Credit","Score","Events"], rows, "2fr 1fr 0.8fr 0.8fr"), unsafe_allow_html=True)
            else:
                st.success("No high-risk customers found. Portfolio is clean.")
        except Exception as e:
            st.error(str(e))

    st.markdown("---")

    # ── Severity trend + flagged by ───────────────────────────────────────
    col1, col2 = st.columns(2, gap="large")
    with col1:
        section("Risk Event Trend by Severity", f"Last {days} days")
        try:
            rt = q(f"""
            SELECT report_date, severity, SUM(total_events) AS events
            FROM FINANCE_ETL_DEMO.ANALYTICS.RISK_DASHBOARD
            WHERE report_date >= DATEADD('day',-{days},CURRENT_DATE())
            GROUP BY 1,2 ORDER BY 1""")
            if not rt.empty:
                rt.columns = [c.upper() for c in rt.columns]
                pivot = rt.pivot_table(index="REPORT_DATE",columns="SEVERITY",values="EVENTS",aggfunc="sum").fillna(0)
                st.area_chart(pivot, height=220)
        except Exception as e:
            st.error(str(e))

    with col2:
        section("Flagged By: Detection Source", "Rule engine vs ML model vs Manual")
        try:
            flag_df = q("""
            SELECT flagged_by,
                   COUNT(*) AS total,
                   ROUND(AVG(score),1) AS avg_score,
                   SUM(CASE WHEN resolved THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS res_rate
            FROM FINANCE_ETL_DEMO.STAGING.RISK_EVENTS
            GROUP BY 1 ORDER BY 2 DESC""")
            if not flag_df.empty:
                flag_df.columns = [c.upper() for c in flag_df.columns]
                flag_colors = {"ML_MODEL":"#38bdf8","RULE_ENGINE":"#a78bfa","MANUAL":"#fbbf24"}
                rows = []
                for _, r in flag_df.iterrows():
                    fb = str(r.get("FLAGGED_BY",""))
                    fc = flag_colors.get(fb,"#7a8fa8")
                    rr = float(r.get("RES_RATE",0) or 0)
                    rows.append([
                        tag(fb, fc, fc+"22"),
                        mono(fmt(r.get("TOTAL",0)),"#c8d8ec"),
                        mono(str(r.get("AVG_SCORE","—")),"#a78bfa"),
                        f"""<div style="display:flex;align-items:center;gap:8px">
                            <div style="flex:1;height:3px;background:#141e2e;border-radius:2px">
                                <div style="width:{rr:.0f}%;height:100%;background:#4ade80;border-radius:2px"></div>
                            </div>
                            <span style="font-family:IBM Plex Mono,monospace;font-size:11px;color:#4ade80;min-width:36px">{rr:.0f}%</span>
                        </div>""",
                    ])
                st.markdown(table_html(["Source","Events","Avg Score","Resolution"], rows, "1.5fr 0.8fr 0.9fr 2fr"), unsafe_allow_html=True)
        except Exception as e:
            st.error(str(e))

    st.markdown("---")

    # ── AI Cortex anomaly section ──────────────────────────────────────────
    section("Cortex AI Anomaly Explanations", "LLM-powered risk narrative — generated inside Snowflake")
    try:
        adf = q("""SELECT event_type, last_7d_events, ROUND(spike_ratio,2) AS spike_ratio,
                   is_spike, cortex_analysis
                   FROM FINANCE_ETL_DEMO.ANALYTICS.RISK_ANOMALY_LOG
                   ORDER BY spike_ratio DESC LIMIT 5""")
        if not adf.empty:
            adf.columns = [c.upper() for c in adf.columns]
            for _, r in adf.iterrows():
                is_spike = bool(r.get("IS_SPIKE",False))
                bc = "#f87171" if is_spike else "#141e2e"
                analysis = str(r.get("CORTEX_ANALYSIS","") or "")
                st.markdown(f"""
                <div style="background:#0a0f18;border:1px solid {bc}44;border-left:3px solid {bc};
                            border-radius:9px;padding:14px 18px;margin-bottom:10px">
                    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
                        <div style="display:flex;align-items:center;gap:10px">
                            <span style="font-family:IBM Plex Mono,monospace;font-size:13px;color:#c8d8ec;font-weight:500">{r.get("EVENT_TYPE","")}</span>
                            {tag("⚡ SPIKE DETECTED","#f87171","#f8717122") if is_spike else tag("NORMAL","#3d5066","#141e2e")}
                        </div>
                        <span style="font-family:IBM Plex Mono,monospace;font-size:13px;color:{'#f87171' if is_spike else '#3d5066'}">{r.get("SPIKE_RATIO","—")}× baseline</span>
                    </div>
                    {f'<div style="background:#060a10;border-radius:6px;padding:10px 14px;font-size:12px;color:#7a8fa8;line-height:1.7;font-style:italic;margin-top:8px">{analysis}</div>' if analysis else ''}
                </div>""", unsafe_allow_html=True)
        else:
            info_box("Run <strong>CoCo Prompt 3</strong> to enable Cortex LLM anomaly detection. Once active, AI-generated explanations for every risk spike will appear here.", "#a78bfa", "#1a0d2b", "🤖")
    except Exception as e:
        info_box("Anomaly log not yet created — run CoCo Prompt 3 to enable this feature.", "#a78bfa", "#1a0d2b", "🤖")


# ══════════════════════════════════════════════════════════════════════════
# TAB 4 — CUSTOMER 360
# ══════════════════════════════════════════════════════════════════════════
with tab4:
    section("Customer 360°", "Complete customer financial profile — the finance industry's holy grail")

    info_box("Search any customer by ID or name to see their complete financial profile: all accounts, transaction history, risk events, credit health, and AI risk assessment — all joined automatically from Bronze through Gold.", "#4ade80", "#071a0e", "👤")

    # ── Search ────────────────────────────────────────────────────────────
    col_s1, col_s2 = st.columns([2, 1], gap="large")
    with col_s1:
        search_q = st.text_input("Search customer by name or ID", placeholder="e.g. James Smith or CUST-000042")
    with col_s2:
        if st.button("🔍  Look up Customer", type="primary"):
            st.session_state["c360_search"] = search_q

    search_term = st.session_state.get("c360_search", "")
    if search_term:
        try:
            safe = search_term.replace("'","''")
            cust_df = q(f"""
            SELECT ch.customer_id, ch.full_name, c.email, c.phone, c.age,
                   c.country, c.city, c.credit_score, ch.credit_tier,
                   c.annual_income, ch.income_bracket, c.customer_segment,
                   c.kyc_status, ch.is_verified,
                   ch.total_accounts, ch.active_accounts,
                   ROUND(ch.total_balance_usd,2) AS total_balance,
                   ROUND(ch.avg_balance_usd,2) AS avg_balance,
                   ch.total_txn_count, ROUND(ch.total_txn_volume_usd,2) AS txn_volume,
                   ROUND(ch.avg_txn_amount_usd,2) AS avg_txn,
                   ch.last_txn_date,
                   ch.risk_event_count, ch.critical_risk_count,
                   ROUND(ch.risk_score_avg,1) AS risk_score, ch.customer_risk_tier
            FROM FINANCE_ETL_DEMO.ANALYTICS.CUSTOMER_FINANCIAL_HEALTH ch
            JOIN FINANCE_ETL_DEMO.STAGING.CUSTOMERS c ON ch.customer_id=c.customer_id
            WHERE LOWER(ch.full_name) LIKE LOWER('%{safe}%')
               OR LOWER(ch.customer_id) LIKE LOWER('%{safe}%')
            LIMIT 5""")

            if cust_df.empty:
                st.warning(f"No customer found matching '{search_term}'")
            else:
                cust_df.columns = [c.upper() for c in cust_df.columns]

                if len(cust_df) > 1:
                    names = [f"{r['FULL_NAME']} ({r['CUSTOMER_ID']})" for _, r in cust_df.iterrows()]
                    selected = st.selectbox("Multiple matches found — select one:", names)
                    idx = names.index(selected)
                    c = cust_df.iloc[idx]
                else:
                    c = cust_df.iloc[0]

                cid = c["CUSTOMER_ID"]
                risk_tier = str(c.get("CUSTOMER_RISK_TIER",""))
                risk_color = {"HIGH_RISK":"#f87171","MEDIUM_RISK":"#fbbf24","LOW_RISK":"#4ade80"}.get(risk_tier,"#7a8fa8")

                # ── Profile header ────────────────────────────────────────
                st.markdown(f"""
                <div style="background:#0a0f18;border:1px solid #141e2e;border-radius:12px;
                            padding:24px 28px;margin-bottom:1.5rem">
                    <div style="display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:16px">
                        <div>
                            <div style="font-family:'Syne',sans-serif;font-size:26px;font-weight:600;
                                        color:#e2eaf4;margin-bottom:4px">{c.get("FULL_NAME","")}</div>
                            <div style="font-family:IBM Plex Mono,monospace;font-size:12px;
                                        color:#3d5066;margin-bottom:12px">{cid}</div>
                            <div style="display:flex;gap:8px;flex-wrap:wrap">
                                {tag(str(c.get("CUSTOMER_SEGMENT","")), "#38bdf8", "#38bdf822")}
                                {tag(str(c.get("CREDIT_TIER","")), "#4ade80", "#4ade8022")}
                                {tag(str(c.get("INCOME_BRACKET",""))+" INCOME", "#a78bfa", "#a78bfa22")}
                                {tag("✓ KYC VERIFIED" if c.get("IS_VERIFIED") else "⚠ KYC UNVERIFIED", "#4ade80" if c.get("IS_VERIFIED") else "#fbbf24", "#071a0e" if c.get("IS_VERIFIED") else "#1a1007")}
                            </div>
                        </div>
                        <div style="background:#060a10;border:1px solid {risk_color}33;border-radius:10px;
                                    padding:16px 24px;text-align:center">
                            <div style="font-size:10px;color:#3d5066;text-transform:uppercase;
                                        letter-spacing:0.1em;font-weight:700;margin-bottom:6px">Risk Score</div>
                            <div style="font-family:'Syne',sans-serif;font-size:42px;font-weight:700;
                                        color:{risk_color};line-height:1">{c.get("RISK_SCORE","—")}</div>
                            <div style="font-size:11px;color:{risk_color};font-weight:600;
                                        letter-spacing:0.06em;margin-top:4px">{risk_tier.replace("_"," ")}</div>
                        </div>
                    </div>
                    <div style="border-top:1px solid #141e2e;margin-top:18px;padding-top:14px;
                                display:grid;grid-template-columns:repeat(4,1fr);gap:16px">
                        <div><div style="font-size:10px;color:#3d5066;text-transform:uppercase;letter-spacing:0.08em;font-weight:600;margin-bottom:4px">Location</div>
                             <div style="font-size:13px;color:#c8d8ec">{c.get("CITY","")}, {c.get("COUNTRY","")}</div></div>
                        <div><div style="font-size:10px;color:#3d5066;text-transform:uppercase;letter-spacing:0.08em;font-weight:600;margin-bottom:4px">Age</div>
                             <div style="font-size:13px;color:#c8d8ec">{c.get("AGE","—")} years</div></div>
                        <div><div style="font-size:10px;color:#3d5066;text-transform:uppercase;letter-spacing:0.08em;font-weight:600;margin-bottom:4px">Credit Score</div>
                             <div style="font-size:13px;font-family:IBM Plex Mono,monospace;color:#38bdf8">{c.get("CREDIT_SCORE","—")}</div></div>
                        <div><div style="font-size:10px;color:#3d5066;text-transform:uppercase;letter-spacing:0.08em;font-weight:600;margin-bottom:4px">Annual Income</div>
                             <div style="font-size:13px;font-family:IBM Plex Mono,monospace;color:#4ade80">${float(c.get("ANNUAL_INCOME",0) or 0):,.0f}</div></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # ── Financial summary ─────────────────────────────────────
                col_a, col_b, col_c = st.columns(3, gap="medium")
                with col_a:
                    st.markdown(f"""
                    <div style="background:#0a0f18;border:1px solid #141e2e;border-radius:10px;padding:16px">
                        <div style="font-size:10px;color:#3d5066;text-transform:uppercase;letter-spacing:0.1em;font-weight:700;margin-bottom:12px">💳 Accounts</div>
                        {kv_row("Total accounts", fmt(c.get("TOTAL_ACCOUNTS",0)), "#c8d8ec")}
                        {kv_row("Active accounts", fmt(c.get("ACTIVE_ACCOUNTS",0)), "#4ade80")}
                        {kv_row("Total balance", f'${float(c.get("TOTAL_BALANCE",0) or 0):,.2f}', "#38bdf8")}
                        {kv_row("Avg balance", f'${float(c.get("AVG_BALANCE",0) or 0):,.2f}', "#7a8fa8")}
                    </div>""", unsafe_allow_html=True)
                with col_b:
                    st.markdown(f"""
                    <div style="background:#0a0f18;border:1px solid #141e2e;border-radius:10px;padding:16px">
                        <div style="font-size:10px;color:#3d5066;text-transform:uppercase;letter-spacing:0.1em;font-weight:700;margin-bottom:12px">💸 Transactions</div>
                        {kv_row("Total transactions", fmt(c.get("TOTAL_TXN_COUNT",0)), "#c8d8ec")}
                        {kv_row("Total volume", f'${float(c.get("TXN_VOLUME",0) or 0):,.2f}', "#38bdf8")}
                        {kv_row("Avg transaction", f'${float(c.get("AVG_TXN",0) or 0):,.2f}', "#a78bfa")}
                        {kv_row("Last transaction", str(c.get("LAST_TXN_DATE","—")), "#7a8fa8")}
                    </div>""", unsafe_allow_html=True)
                with col_c:
                    st.markdown(f"""
                    <div style="background:#0a0f18;border:1px solid #141e2e;border-top:2px solid {risk_color}44;border-radius:10px;padding:16px">
                        <div style="font-size:10px;color:#3d5066;text-transform:uppercase;letter-spacing:0.1em;font-weight:700;margin-bottom:12px">🛡️ Risk Profile</div>
                        {kv_row("Risk events", fmt(c.get("RISK_EVENT_COUNT",0)), risk_color)}
                        {kv_row("Critical events", fmt(c.get("CRITICAL_RISK_COUNT",0)), "#f87171")}
                        {kv_row("Avg risk score", str(c.get("RISK_SCORE","—")), risk_color)}
                        {kv_row("Risk tier", risk_tier.replace("_"," "), risk_color)}
                    </div>""", unsafe_allow_html=True)

                # ── Account list ──────────────────────────────────────────
                st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
                section("Account Details", f"All accounts for {c.get('FULL_NAME','')}")
                try:
                    acct_df = q(f"""
                    SELECT account_id, account_type, currency, balance, balance_bucket,
                           status, is_active, interest_rate, account_age_days, credit_limit
                    FROM FINANCE_ETL_DEMO.STAGING.ACCOUNTS
                    WHERE customer_id='{cid}'
                    ORDER BY balance DESC""")
                    if not acct_df.empty:
                        acct_df.columns = [c2.upper() for c2 in acct_df.columns]
                        rows = []
                        for _, r in acct_df.iterrows():
                            st_val = str(r.get("STATUS",""))
                            st_c = "#4ade80" if st_val=="ACTIVE" else "#f87171"
                            bal = float(r.get("BALANCE",0) or 0)
                            bal_c = "#f87171" if bal<0 else "#4ade80" if bal>50000 else "#c8d8ec"
                            rows.append([
                                mono(str(r.get("ACCOUNT_ID","")), "#7a8fa8","11px"),
                                tag(str(r.get("ACCOUNT_TYPE","")), "#38bdf8", "#38bdf822"),
                                f'<span style="font-family:IBM Plex Mono,monospace;font-size:12px;color:{bal_c}">${bal:,.2f}</span>',
                                mono(str(r.get("CURRENCY","")), "#a78bfa"),
                                f'<span style="font-size:11px;color:{st_c};font-weight:500">{st_val}</span>',
                                mono(f'{float(r.get("INTEREST_RATE",0) or 0)*100:.2f}%', "#fbbf24"),
                            ])
                        st.markdown(table_html(["Account ID","Type","Balance","Currency","Status","Rate"], rows, "2fr 1.2fr 1.5fr 0.8fr 0.9fr 0.8fr"), unsafe_allow_html=True)
                except Exception as e2:
                    st.error(str(e2))

                # ── Recent transactions for this customer ─────────────────
                st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
                section("Recent Transactions", f"Last 15 transactions")
                try:
                    txn_df = q(f"""
                    SELECT t.transaction_id, t.transaction_type, t.amount_usd, t.currency,
                           t.merchant_name, t.merchant_category, t.channel, t.status,
                           t.is_large_txn, TO_CHAR(t.transaction_dt,'YYYY-MM-DD') AS dt
                    FROM FINANCE_ETL_DEMO.STAGING.TRANSACTIONS t
                    WHERE t.customer_id='{cid}'
                    ORDER BY t.transaction_dt DESC LIMIT 15""")
                    if not txn_df.empty:
                        txn_df.columns = [c2.upper() for c2 in txn_df.columns]
                        rows = []
                        for _, r in txn_df.iterrows():
                            sc3 = "#4ade80" if r.get("STATUS")=="COMPLETED" else "#f87171" if r.get("STATUS")=="FAILED" else "#fbbf24"
                            amt = float(r.get("AMOUNT_USD",0) or 0)
                            large_badge = tag("LARGE","#fbbf24","#fbbf2422") if r.get("IS_LARGE_TXN") else ""
                            rows.append([
                                mono(str(r.get("DT","")), "#3d5066","11px"),
                                f'<span style="font-size:11px;color:#c8d8ec">{r.get("MERCHANT_NAME","")}</span>',
                                tag(str(r.get("MERCHANT_CATEGORY","")), "#7a8fa8", "#141e2e"),
                                f'<span style="font-family:IBM Plex Mono,monospace;font-size:12px;color:#38bdf8">${amt:,.2f}</span>{large_badge}',
                                f'<span style="font-size:11px;color:{sc3};font-weight:500">{r.get("STATUS","")}</span>',
                            ])
                        st.markdown(table_html(["Date","Merchant","Category","Amount","Status"], rows, "1.2fr 1.5fr 1.2fr 1.5fr 1fr"), unsafe_allow_html=True)
                    else:
                        st.info("No transactions found for this customer.")
                except Exception as e3:
                    st.error(str(e3))

        except Exception as e:
            st.error(f"Customer lookup error: {e}")
    else:
        st.markdown("""
        <div style="background:#0a0f18;border:1px solid #141e2e;border-radius:12px;
                    padding:60px;text-align:center;margin-top:1rem">
            <div style="font-size:40px;margin-bottom:16px">👤</div>
            <div style="font-family:'Syne',sans-serif;font-size:18px;color:#c8d8ec;font-weight:400;margin-bottom:8px">
                Search for any customer
            </div>
            <div style="font-size:13px;color:#3d5066;max-width:400px;margin:0 auto;line-height:1.6">
                Enter a customer name (e.g. <span style="color:#7a8fa8;font-family:IBM Plex Mono,monospace">James Smith</span>) 
                or ID (e.g. <span style="color:#7a8fa8;font-family:IBM Plex Mono,monospace">CUST-000042</span>) 
                to see their complete 360° financial profile.
            </div>
        </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
# TAB 5 — CORTEX AI ANALYST
# ══════════════════════════════════════════════════════════════════════════
with tab5:
    section("Cortex AI Analyst", "Ask questions about your financial data in plain English")

    info_box("""<strong style='color:#38bdf8'>How this works:</strong> Your question is sent to 
    <code style='font-family:IBM Plex Mono,monospace;font-size:11px;background:#141e2e;padding:1px 5px;border-radius:3px'>SNOWFLAKE.CORTEX.COMPLETE</code> 
    along with context about the database schema. The AI understands your data model and generates insights — 
    all inside Snowflake. No data leaves the platform.""", "#38bdf8", "#071222", "🤖")

    # ── Suggested questions ────────────────────────────────────────────────
    st.markdown("""
    <div style="font-size:10px;color:#3d5066;font-weight:700;letter-spacing:0.1em;
                text-transform:uppercase;margin-bottom:10px">Suggested questions</div>
    """, unsafe_allow_html=True)

    suggestions = [
        "Which merchant categories have the highest fraud risk?",
        "What is the profile of a typical high-risk customer?",
        "How does the PREMIUM segment compare to RETAIL in transaction behavior?",
        "What are the main patterns in CRITICAL risk events?",
        "Explain what Bronze → Silver → Gold means in this pipeline",
        "What business insights can we get from the ANALYTICS layer?",
    ]
    cols_s = st.columns(3)
    for i, sug in enumerate(suggestions):
        with cols_s[i % 3]:
            if st.button(sug, key=f"sug_{i}"):
                st.session_state["ai_question"] = sug

    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

    # ── Question input ─────────────────────────────────────────────────────
    question = st.text_area(
        "Or type your own question:",
        value=st.session_state.get("ai_question",""),
        height=90,
        placeholder="e.g. Which customers are most likely to churn based on their transaction history?",
        key="ai_q_input"
    )

    col_btn1, col_btn2 = st.columns([1, 4])
    with col_btn1:
        ask = st.button("🤖  Ask Cortex AI", type="primary", use_container_width=True)
    with col_btn2:
        if st.button("🗑  Clear", use_container_width=False):
            st.session_state.pop("ai_question", None)
            st.session_state.pop("ai_answer", None)
            st.rerun()

    if ask and question.strip():
        st.session_state["ai_question"] = question

        schema_context = """
You are a financial data analyst AI assistant embedded inside a Snowflake data platform called "Autonomous Finance ETL Agent".

DATABASE: FINANCE_ETL_DEMO
SCHEMA LAYERS:
- RAW (Bronze): CUSTOMERS(2000 rows), ACCOUNTS(5000 rows), TRANSACTIONS(100000 rows), RISK_EVENTS(3000 rows) - raw ingested data
- STAGING (Silver): cleaned & enriched versions of the same tables with derived columns like credit_tier, balance_bucket, is_large_txn, severity_rank
- ANALYTICS (Gold): DAILY_TXN_SUMMARY, CUSTOMER_FINANCIAL_HEALTH (360-degree customer view), RISK_DASHBOARD, RISK_ANOMALY_LOG (Cortex LLM anomaly analysis)
- AUDIT: ETL_RUN_LOG (every pipeline run), DATA_QUALITY_LOG (data quality check results)

KEY BUSINESS CONTEXT:
- Finance domain: customers have multiple accounts (CHECKING/SAVINGS/CREDIT/LOAN)
- Transactions: DEPOSIT/WITHDRAWAL/TRANSFER/PAYMENT/FEE across ONLINE/ATM/MOBILE/BRANCH channels
- Risk events: FRAUD_ALERT/LARGE_TRANSACTION/UNUSUAL_LOCATION/MULTIPLE_FAILED_ATTEMPTS/VELOCITY_BREACH/ACCOUNT_TAKEOVER_SIGNAL
- Customer segments: RETAIL/PREMIUM/BUSINESS
- Credit tiers: EXCELLENT(750+)/GOOD(670+)/FAIR(580+)/POOR(<580)
- The pipeline runs every 15 minutes autonomously using Snowflake Tasks and Streams (CDC)
- Built 100% in Snowflake using CoCo (Cortex Code) — no external tools

When answering, be specific, reference the actual table and column names, provide actionable insights, and explain the business implications. Format your response clearly with sections if needed."""

        with st.spinner("Cortex AI is analyzing your data…"):
            try:
                safe_q = question.replace("'", "''").replace("\\", "\\\\")
                full_prompt = f"{schema_context}\n\nUser question: {safe_q}"
                result_df = session.sql(f"""
                    SELECT SNOWFLAKE.CORTEX.COMPLETE(
                        'snowflake-arctic',
                        '{full_prompt}'
                    ) AS answer
                """).to_pandas()
                answer = result_df.iloc[0]["ANSWER"] if not result_df.empty else "No response received."
                st.session_state["ai_answer"] = answer
                st.session_state["ai_last_q"] = question
            except Exception as e:
                st.session_state["ai_answer"] = f"⚠ Cortex API error: {str(e)}\n\nMake sure SNOWFLAKE.CORTEX.COMPLETE is available in your Snowflake account."

    if st.session_state.get("ai_answer"):
        st.markdown(f"""
        <div style="margin-top:1.5rem">
            <div style="font-size:10px;color:#3d5066;font-weight:700;letter-spacing:0.1em;
                        text-transform:uppercase;margin-bottom:8px">
                🤖 Cortex AI Response
            </div>
            <div style="background:#0a0f18;border:1px solid #38bdf822;border-left:3px solid #38bdf8;
                        border-radius:10px;padding:20px 24px">
                <div style="font-size:11px;color:#38bdf8;font-weight:700;letter-spacing:0.08em;
                            text-transform:uppercase;margin-bottom:10px">
                    Question: {st.session_state.get('ai_last_q','')}
                </div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown(st.session_state["ai_answer"])
        st.markdown("""
        <div style="background:#060a10;border-radius:0 0 10px 10px;padding:8px 24px;
                    border:1px solid #38bdf822;border-top:none;
                    font-size:10px;color:#3d5066;font-family:IBM Plex Mono,monospace">
            ✦ Generated by SNOWFLAKE.CORTEX.COMPLETE · snowflake-arctic model · data never left Snowflake
        </div>
        """, unsafe_allow_html=True)

    # ── What this proves to judges ─────────────────────────────────────────
    st.markdown("---")
    section("Why this matters", "The business case for AI-native data platforms")
    st.markdown("""
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px">
        <div style="background:#0a0f18;border:1px solid #141e2e;border-radius:9px;padding:18px">
            <div style="font-size:20px;margin-bottom:10px">🚫</div>
            <div style="font-size:13px;color:#e2eaf4;font-weight:500;margin-bottom:6px">Before</div>
            <div style="font-size:12px;color:#3d5066;line-height:1.7">
                Business analyst writes a ticket. Data engineer writes SQL. Review cycle. 
                3 days later: a static report that's already stale.
            </div>
        </div>
        <div style="background:#071222;border:1px solid #38bdf822;border-left:3px solid #38bdf8;
                    border-radius:9px;padding:18px">
            <div style="font-size:20px;margin-bottom:10px">✅</div>
            <div style="font-size:13px;color:#38bdf8;font-weight:500;margin-bottom:6px">After (This Platform)</div>
            <div style="font-size:12px;color:#7a8fa8;line-height:1.7">
                Business analyst types a question in plain English. Cortex AI answers instantly 
                with real data context. Zero tickets. Zero SQL. Zero waiting.
            </div>
        </div>
        <div style="background:#0a0f18;border:1px solid #141e2e;border-radius:9px;padding:18px">
            <div style="font-size:20px;margin-bottom:10px">🏆</div>
            <div style="font-size:13px;color:#e2eaf4;font-weight:500;margin-bottom:6px">The differentiator</div>
            <div style="font-size:12px;color:#3d5066;line-height:1.7">
                Everything runs inside Snowflake. No data governance risk. No API keys. 
                No external systems. Full compliance, full audit trail, zero infrastructure.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
# TAB 6 — PIPELINE MONITOR
# ══════════════════════════════════════════════════════════════════════════
with tab6:
    section("Pipeline Monitor", "ETL execution health, data quality, and row counts")

    mon_tab1, mon_tab2, mon_tab3, mon_tab4 = st.tabs([
        "📊 Run Summary",
        "📋 Run History",
        "✅ Data Quality",
        "🗄️ Row Counts"
    ])

    with mon_tab1:
        # ── KPI row ───────────────────────────────────────────────────────
        try:
            kpi = q(f"""
            SELECT COUNT(*) AS runs, SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS ok,
                   SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS fail,
                   ROUND(AVG(duration_sec),1) AS avg_dur, ROUND(MAX(duration_sec),1) AS max_dur,
                   ROUND(SUM(CASE WHEN status='SUCCESS' THEN 1.0 ELSE 0 END)/NULLIF(COUNT(*),0)*100,1) AS pct,
                   SUM(COALESCE(rows_inserted,0)) AS total_rows
            FROM FINANCE_ETL_DEMO.AUDIT.ETL_RUN_LOG
            WHERE started_at >= DATEADD('day',-{days},CURRENT_TIMESTAMP())
              AND procedure_name='SP_AUTONOMOUS_ETL_AGENT'""").iloc[0]
            cols = st.columns(5)
            kpis = [
                ("Total Runs", fmt(kpi.get("RUNS",0))),
                ("Successful", fmt(kpi.get("OK",0))),
                ("Failed", fmt(kpi.get("FAIL",0))),
                ("Success Rate", f"{kpi.get('PCT',0) or 0}%"),
                ("Avg Duration", f"{kpi.get('AVG_DUR',0) or 0}s"),
            ]
            for i,(lbl,val) in enumerate(kpis):
                with cols[i]: st.metric(lbl, val)
        except Exception as e:
            st.error(str(e))

        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

        # ── Procedure health ──────────────────────────────────────────────
        try:
            proc_df = q(f"""
            SELECT procedure_name, layer,
                   COUNT(*) AS runs, SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS ok,
                   SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS fail,
                   ROUND(AVG(duration_sec),2) AS avg_s, ROUND(MAX(duration_sec),2) AS max_s,
                   SUM(COALESCE(rows_inserted,0)) AS rows,
                   ROUND(SUM(CASE WHEN status='SUCCESS' THEN 1.0 ELSE 0 END)/NULLIF(COUNT(*),0)*100,1) AS pct
            FROM FINANCE_ETL_DEMO.AUDIT.ETL_RUN_LOG
            WHERE started_at >= DATEADD('day',-{days},CURRENT_TIMESTAMP())
            GROUP BY 1,2 ORDER BY 2,1""")
            if not proc_df.empty:
                proc_df.columns = [c.upper() for c in proc_df.columns]
                rows_h = []
                for _, r in proc_df.iterrows():
                    pct = float(r.get("PCT",0) or 0)
                    bc = "#4ade80" if pct==100 else "#fbbf24" if pct>=80 else "#f87171"
                    rows_h.append([
                        mono(str(r.get("PROCEDURE_NAME","")), "#c8d8ec","12px"),
                        tag(str(r.get("LAYER","")), "#7a8fa8","#141e2e"),
                        mono(fmt(r.get("RUNS",0)), "#7a8fa8"),
                        mono(fmt(r.get("OK",0)), "#4ade80"),
                        mono(fmt(r.get("FAIL",0)), "#f87171"),
                        mono(f'{r.get("AVG_S","—")}s', "#7a8fa8"),
                        mono(fmt(r.get("ROWS",0)), "#a78bfa"),
                        f"""<div style="display:flex;align-items:center;gap:8px">
                            <div style="flex:1;height:3px;background:#141e2e;border-radius:2px">
                                <div style="width:{pct}%;height:100%;background:{bc};border-radius:2px"></div>
                            </div>
                            <span style="font-family:IBM Plex Mono,monospace;font-size:11px;color:{bc};min-width:38px">{pct}%</span>
                        </div>""",
                    ])
                st.markdown(table_html(["Procedure","Layer","Runs","OK","Fail","Avg","Rows","Success Rate"], rows_h, "2.5fr 0.9fr 0.6fr 0.6fr 0.6fr 0.7fr 0.9fr 1.5fr"), unsafe_allow_html=True)
        except Exception as e:
            st.error(str(e))

        # ── Timeline ──────────────────────────────────────────────────────
        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
        try:
            tl = q(f"""
            SELECT DATE_TRUNC('hour',started_at) AS run_hour,
                   SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS ok,
                   SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS err
            FROM FINANCE_ETL_DEMO.AUDIT.ETL_RUN_LOG
            WHERE started_at >= DATEADD('day',-{days},CURRENT_TIMESTAMP())
              AND procedure_name='SP_AUTONOMOUS_ETL_AGENT'
            GROUP BY 1 ORDER BY 1""")
            if not tl.empty:
                tl.columns = [c.upper() for c in tl.columns]
                st.bar_chart(tl.rename(columns={"RUN_HOUR":"Hour"}).set_index("Hour")[["OK","ERR"]], color=["#38bdf8","#f87171"], height=180)
        except Exception as e:
            st.error(str(e))

    with mon_tab2:
        col1,col2,col3 = st.columns(3)
        with col1: lf = st.selectbox("Layer",["ALL","STAGING","ANALYTICS","RAW","AUDIT"])
        with col2: sf = st.selectbox("Status",["ALL","SUCCESS","FAILED","SKIPPED"])
        with col3: ln = st.number_input("Rows",10,500,50,10)

        wc = [f"started_at >= DATEADD('day',-{days},CURRENT_TIMESTAMP())"]
        if lf!="ALL": wc.append(f"layer='{lf}'")
        if sf!="ALL": wc.append(f"status='{sf}'")
        ws = " AND ".join(wc)

        try:
            hd = q(f"""
            SELECT TO_CHAR(started_at,'YYYY-MM-DD HH24:MI:SS') AS ts, procedure_name,
                   layer, status, COALESCE(rows_inserted,0) AS rows,
                   ROUND(duration_sec,2) AS dur,
                   SUBSTR(COALESCE(error_message,''),1,100) AS err
            FROM FINANCE_ETL_DEMO.AUDIT.ETL_RUN_LOG WHERE {ws}
            ORDER BY started_at DESC LIMIT {ln}""")
            if not hd.empty:
                hd.columns = [c.upper() for c in hd.columns]
                rows_h = []
                for _, r in hd.iterrows():
                    s = str(r.get("STATUS",""))
                    sc4 = {"SUCCESS":"#4ade80","FAILED":"#f87171"}.get(s,"#fbbf24")
                    err_html = f'<div style="font-size:10px;color:#f87171;font-family:IBM Plex Mono,monospace">{r.get("ERR","")[:60]}</div>' if r.get("ERR") else ""
                    rows_h.append([
                        mono(str(r.get("TS","")), "#3d5066","10px"),
                        f'<div>{mono(str(r.get("PROCEDURE_NAME","")), "#c8d8ec","11px")}{err_html}</div>',
                        tag(str(r.get("LAYER","")), "#7a8fa8","#141e2e"),
                        f'<div style="display:flex;align-items:center;gap:5px"><div style="width:4px;height:4px;border-radius:50%;background:{sc4}"></div><span style="font-size:11px;color:{sc4};font-weight:500">{s}</span></div>',
                        mono(fmt(r.get("ROWS",0)), "#a78bfa"),
                        mono(f'{r.get("DUR","—")}s', "#3d5066"),
                    ])
                st.markdown(table_html(["Timestamp","Procedure","Layer","Status","Rows","Dur"], rows_h, "2fr 2fr 0.8fr 1fr 0.9fr 0.7fr"), unsafe_allow_html=True)
        except Exception as e:
            st.error(str(e))

    with mon_tab3:
        try:
            dq = q(f"""
            SELECT table_name, check_name, check_type, passed, failed_rows, total_rows,
                   ROUND(failed_rows*100.0/NULLIF(total_rows,0),2) AS fail_pct,
                   TO_CHAR(checked_at,'YYYY-MM-DD HH24:MI') AS checked_at
            FROM FINANCE_ETL_DEMO.AUDIT.DATA_QUALITY_LOG
            WHERE checked_at >= DATEADD('day',-{days},CURRENT_TIMESTAMP())
            ORDER BY checked_at DESC""")
            if not dq.empty:
                dq.columns = [c.upper() for c in dq.columns]
                total_c = len(dq); passed_c = int(dq["PASSED"].sum())
                cols = st.columns(4)
                for i,(lbl,val,ac) in enumerate([
                    ("Total checks",fmt(total_c),"#38bdf8"),
                    ("Passed",fmt(passed_c),"#4ade80"),
                    ("Failed",fmt(total_c-passed_c),"#f87171"),
                    ("Pass rate",f"{passed_c/total_c*100:.1f}%" if total_c>0 else "—","#a78bfa"),
                ]):
                    with cols[i]: st.markdown(card(lbl,val,"",ac), unsafe_allow_html=True)
                st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
                rows_h=[]
                for _, r in dq.iterrows():
                    ok = bool(r.get("PASSED",False))
                    ic = "#4ade80" if ok else "#f87171"
                    rows_h.append([
                        f'<span style="font-size:14px;font-family:IBM Plex Mono,monospace;color:{ic};font-weight:600">{"✓" if ok else "✗"}</span>',
                        mono(str(r.get("TABLE_NAME","")), "#7a8fa8","11px"),
                        f'<span style="font-size:12px;color:#c8d8ec">{r.get("CHECK_NAME","")}</span>',
                        tag(str(r.get("CHECK_TYPE","")), "#7a8fa8","#141e2e"),
                        mono(fmt(r.get("FAILED_ROWS",0)), ic if not ok else "#3d5066"),
                    ])
                st.markdown(table_html(["","Table","Check","Type","Failed Rows"], rows_h, "0.3fr 1.5fr 2fr 1.2fr 0.9fr"), unsafe_allow_html=True)
            else:
                st.info("No data quality records in this window.")
        except Exception as e:
            st.error(str(e))

    with mon_tab4:
        try:
            cts = q("""
            SELECT 'RAW' AS L,'CUSTOMERS' AS T,COUNT(*) AS N FROM FINANCE_ETL_DEMO.RAW.CUSTOMERS UNION ALL
            SELECT 'RAW','ACCOUNTS',COUNT(*) FROM FINANCE_ETL_DEMO.RAW.ACCOUNTS UNION ALL
            SELECT 'RAW','TRANSACTIONS',COUNT(*) FROM FINANCE_ETL_DEMO.RAW.TRANSACTIONS UNION ALL
            SELECT 'RAW','RISK_EVENTS',COUNT(*) FROM FINANCE_ETL_DEMO.RAW.RISK_EVENTS UNION ALL
            SELECT 'STAGING','CUSTOMERS',COUNT(*) FROM FINANCE_ETL_DEMO.STAGING.CUSTOMERS UNION ALL
            SELECT 'STAGING','ACCOUNTS',COUNT(*) FROM FINANCE_ETL_DEMO.STAGING.ACCOUNTS UNION ALL
            SELECT 'STAGING','TRANSACTIONS',COUNT(*) FROM FINANCE_ETL_DEMO.STAGING.TRANSACTIONS UNION ALL
            SELECT 'STAGING','RISK_EVENTS',COUNT(*) FROM FINANCE_ETL_DEMO.STAGING.RISK_EVENTS UNION ALL
            SELECT 'ANALYTICS','DAILY_TXN_SUMMARY',COUNT(*) FROM FINANCE_ETL_DEMO.ANALYTICS.DAILY_TXN_SUMMARY UNION ALL
            SELECT 'ANALYTICS','CUSTOMER_FINANCIAL_HEALTH',COUNT(*) FROM FINANCE_ETL_DEMO.ANALYTICS.CUSTOMER_FINANCIAL_HEALTH UNION ALL
            SELECT 'ANALYTICS','RISK_DASHBOARD',COUNT(*) FROM FINANCE_ETL_DEMO.ANALYTICS.RISK_DASHBOARD
            ORDER BY 1,2""")
            cts.columns = [c.upper() for c in cts.columns]
            for layer, emoji, color in [("RAW","🥉","#f59e0b"),("STAGING","🥈","#38bdf8"),("ANALYTICS","🥇","#4ade80")]:
                sub = cts[cts["L"]==layer]
                if sub.empty: continue
                total = int(sub["N"].sum())
                tbl_cols = st.columns(len(sub))
                st.markdown(f'<div style="font-size:13px;color:{color};font-weight:500;margin-bottom:8px">{emoji} {layer} · {total:,} rows total</div>', unsafe_allow_html=True)
                for j,(_,r) in enumerate(sub.iterrows()):
                    with tbl_cols[j]:
                        st.metric(r["T"], f"{int(r['N']):,}")
                st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)
        except Exception as e:
            st.error(str(e))


# ══════════════════════════════════════════════════════════════════════════
# TAB 7 — SQL GENERATOR
# ══════════════════════════════════════════════════════════════════════════
with tab7:
    section("SQL Generator", "Configure parameters — SQL auto-generates as you type")

    info_box("Adjust the parameters on the left. All SQL blocks update instantly. Copy each block into a Snowsight Worksheet and run them in order (Section 0 → 10).", "#38bdf8","#071222","📋")

    col_params, col_sql = st.columns([1, 1], gap="large")

    with col_params:
        st.markdown('<div style="font-size:10px;color:#38bdf8;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:12px;padding:10px 14px;background:#071222;border:1px solid #38bdf822;border-radius:7px">Section [0] — Infrastructure</div>', unsafe_allow_html=True)
        p_db     = st.text_input("Database name", "FINANCE_ETL_DEMO", key="g_db")
        p_wh     = st.text_input("Warehouse name", "ETL_WH", key="g_wh")
        p_size   = st.selectbox("Warehouse size", ["XSMALL","SMALL","MEDIUM","LARGE"], key="g_size")
        p_susp   = st.number_input("Auto-suspend (sec)", 30, 3600, 60, 30, key="g_susp")

        st.markdown('<div style="font-size:10px;color:#4ade80;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;margin:14px 0 12px;padding:10px 14px;background:#071a0e;border:1px solid #4ade8022;border-radius:7px">Section [2] — Mock Data Volume</div>', unsafe_allow_html=True)
        p_cust   = st.number_input("Customers",    100, 50000,  2000, 500,  key="g_cust")
        p_acct   = st.number_input("Accounts",     100, 100000, 5000, 1000, key="g_acct")
        p_txn    = st.number_input("Transactions", 1000,500000,100000,10000,key="g_txn")
        p_risk   = st.number_input("Risk events",  100, 50000,  3000, 500,  key="g_risk")

        st.markdown('<div style="font-size:10px;color:#fbbf24;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;margin:14px 0 12px;padding:10px 14px;background:#1a1007;border:1px solid #fbbf2422;border-radius:7px">Section [8/9] — Agent & Scheduler</div>', unsafe_allow_html=True)
        p_retry  = st.number_input("Max retries", 1, 10, 3, 1, key="g_retry")
        p_sched  = st.selectbox("Task schedule", ["5 MINUTE","10 MINUTE","15 MINUTE","30 MINUTE","1 HOUR"], index=2, key="g_sched")

    with col_sql:
        st.code(f"""USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS {p_wh}
  WAREHOUSE_SIZE = '{p_size}'
  AUTO_SUSPEND   = {p_susp}
  AUTO_RESUME    = TRUE;

CREATE DATABASE IF NOT EXISTS {p_db};
CREATE SCHEMA   IF NOT EXISTS {p_db}.RAW;
CREATE SCHEMA   IF NOT EXISTS {p_db}.STAGING;
CREATE SCHEMA   IF NOT EXISTS {p_db}.ANALYTICS;
CREATE SCHEMA   IF NOT EXISTS {p_db}.AUDIT;

USE WAREHOUSE {p_wh};
USE DATABASE  {p_db};""", language="sql")

        st.code(f"""-- Expected row counts after Section [2]:
--   CUSTOMERS    → {p_cust:,}
--   ACCOUNTS     → {p_acct:,}
--   TRANSACTIONS → {p_txn:,}
--   RISK_EVENTS  → {p_risk:,}
--   TOTAL        → {p_cust+p_acct+p_txn+p_risk:,} rows

SELECT 'CUSTOMERS'   , COUNT(*) FROM {p_db}.RAW.CUSTOMERS    UNION ALL
SELECT 'ACCOUNTS'    , COUNT(*) FROM {p_db}.RAW.ACCOUNTS     UNION ALL
SELECT 'TRANSACTIONS', COUNT(*) FROM {p_db}.RAW.TRANSACTIONS  UNION ALL
SELECT 'RISK_EVENTS' , COUNT(*) FROM {p_db}.RAW.RISK_EVENTS;""", language="sql")

        st.code(f"""-- Section [9] — Scheduled task
CREATE OR REPLACE TASK {p_db}.AUDIT.TASK_ETL_ORCHESTRATOR
  WAREHOUSE = {p_wh}
  SCHEDULE  = '{p_sched}'
AS
  CALL {p_db}.AUDIT.SP_AUTONOMOUS_ETL_AGENT({p_retry}, NULL);

ALTER TASK {p_db}.AUDIT.TASK_ETL_ORCHESTRATOR RESUME;

-- Section [10] — First manual run
CALL {p_db}.AUDIT.SP_AUTONOMOUS_ETL_AGENT({p_retry}, NULL);""", language="sql")

    st.markdown("---")
    section("Full Validation Query", "Run after Section [10]")
    st.code(f"""SELECT tbl, rows FROM (
  SELECT 'RAW.CUSTOMERS'                  AS tbl,COUNT(*) AS rows FROM {p_db}.RAW.CUSTOMERS               UNION ALL
  SELECT 'RAW.ACCOUNTS'                         ,COUNT(*) FROM {p_db}.RAW.ACCOUNTS                        UNION ALL
  SELECT 'RAW.TRANSACTIONS'                     ,COUNT(*) FROM {p_db}.RAW.TRANSACTIONS                    UNION ALL
  SELECT 'RAW.RISK_EVENTS'                      ,COUNT(*) FROM {p_db}.RAW.RISK_EVENTS                     UNION ALL
  SELECT 'STAGING.CUSTOMERS'                    ,COUNT(*) FROM {p_db}.STAGING.CUSTOMERS                   UNION ALL
  SELECT 'STAGING.ACCOUNTS'                     ,COUNT(*) FROM {p_db}.STAGING.ACCOUNTS                    UNION ALL
  SELECT 'STAGING.TRANSACTIONS'                 ,COUNT(*) FROM {p_db}.STAGING.TRANSACTIONS                UNION ALL
  SELECT 'STAGING.RISK_EVENTS'                  ,COUNT(*) FROM {p_db}.STAGING.RISK_EVENTS                 UNION ALL
  SELECT 'ANALYTICS.DAILY_TXN_SUMMARY'          ,COUNT(*) FROM {p_db}.ANALYTICS.DAILY_TXN_SUMMARY         UNION ALL
  SELECT 'ANALYTICS.CUSTOMER_FINANCIAL_HEALTH'  ,COUNT(*) FROM {p_db}.ANALYTICS.CUSTOMER_FINANCIAL_HEALTH UNION ALL
  SELECT 'ANALYTICS.RISK_DASHBOARD'             ,COUNT(*) FROM {p_db}.ANALYTICS.RISK_DASHBOARD
) ORDER BY tbl;""", language="sql")


# ══════════════════════════════════════════════════════════════════════════
# TAB 8 — COCO PROMPTS
# ══════════════════════════════════════════════════════════════════════════
with tab8:
    section("CoCo Pipeline Builder", "Paste these prompts into Snowflake CoCo to build the full pipeline")

    st.markdown("""
    <div style="background:#071222;border:1px solid #38bdf822;border-left:3px solid #38bdf8;
                border-radius:9px;padding:16px 20px;margin-bottom:1.5rem">
        <div style="font-size:10px;color:#38bdf8;font-weight:700;letter-spacing:0.1em;
                    text-transform:uppercase;margin-bottom:8px">CoCo is the star of this project</div>
        <p style="font-size:13px;color:#7a8fa8;line-height:1.7;margin:0">
            Every stored procedure, Gold table, anomaly detection engine, and observability view in this 
            platform was built using <strong style="color:#c8d8ec">natural language prompts</strong> in 
            Snowflake CoCo — no hand-written SQL. Open a Snowsight Worksheet → click the CoCo icon → 
            paste each prompt in order.
        </p>
    </div>
    """, unsafe_allow_html=True)

    COCO_PROMPTS = [
        {
            "num":1,"label":"CDC Stream Test","tag":"Streams · Change Data Capture","accent":"#38bdf8","bg":"#071222",
            "what":"Proves streams work. Inserts 500 rows, checks stream offset, re-runs agent, shows before/after row counts.",
            "prompt":"""Insert 500 new rows into RAW.TRANSACTIONS with transaction dates from today using Snowflake GENERATOR and UNIFORM functions, referencing existing account IDs from STAGING.ACCOUNTS. Then check how many rows are pending in the stream RAW.STR_TRANSACTIONS. Then call AUDIT.SP_AUTONOMOUS_ETL_AGENT(3, NULL) to re-run the pipeline. Finally show me the row counts in STAGING.TRANSACTIONS and ANALYTICS.DAILY_TXN_SUMMARY before and after to confirm the new rows were picked up."""
        },
        {
            "num":2,"label":"Monthly Account Snapshot (LAG window)","tag":"Gold Table · LAG() · MoM Change","accent":"#4ade80","bg":"#071a0e",
            "what":"Adds MONTHLY_ACCOUNT_SNAPSHOT Gold table using LAG() for month-over-month balance change, hooked into the agent as Step 8.",
            "prompt":"""I have a Snowflake database called FINANCE_ETL_DEMO with schemas RAW, STAGING, ANALYTICS, and AUDIT.

Please do the following end to end:
1. Create ANALYTICS.MONTHLY_ACCOUNT_SNAPSHOT with: snapshot_month, account_type, currency, total_accounts, active_accounts, total_balance_usd, avg_balance_usd, min_balance_usd, max_balance_usd, prev_month_balance_usd, mom_change_usd, mom_change_pct, negative_balance_accounts, high_balance_accounts (balance>50000), _etl_loaded_at
2. Create ANALYTICS.SP_BUILD_MONTHLY_ACCOUNT_SNAPSHOT() that truncates/rebuilds using LAG() for MoM change, logs to AUDIT.ETL_RUN_LOG (IMPORTANT: compute v_duration as a variable, bind :v_duration in VALUES not DATEDIFF directly), returns VARIANT with status and rows_inserted
3. Add it as Step 8 in AUDIT.SP_AUTONOMOUS_ETL_AGENT with the same retry loop pattern
4. Run CALL AUDIT.SP_AUTONOMOUS_ETL_AGENT(3, NULL) end to end
5. SELECT * FROM ANALYTICS.MONTHLY_ACCOUNT_SNAPSHOT ORDER BY snapshot_month DESC LIMIT 20"""
        },
        {
            "num":3,"label":"Cortex AI Anomaly Detection","tag":"CORTEX.COMPLETE · Spike Detection · LLM","accent":"#a78bfa","bg":"#0d0722",
            "what":"Adds spike detection (2× baseline) and calls SNOWFLAKE.CORTEX.COMPLETE to generate AI explanations for each risk spike — the centrepiece of the AI angle.",
            "prompt":"""I have FINANCE_ETL_DEMO with ANALYTICS.RISK_DASHBOARD (report_date, event_type, severity, flagged_by, total_events, resolved_events, unresolved_events, avg_risk_score, max_risk_score, resolution_rate_pct).

Please do the following end to end:
1. Create ANALYTICS.RISK_ANOMALY_LOG with: detected_at, event_type, severity DEFAULT 'CRITICAL', last_7d_events, baseline_daily_avg NUMBER(10,2), spike_ratio NUMBER(8,2), is_spike BOOLEAN, cortex_analysis VARCHAR, _etl_loaded_at
2. Create ANALYTICS.SP_DETECT_RISK_ANOMALIES() that:
   - Computes spike_analysis CTE: last_7d_critical_events, baseline_daily_avg (prior 30-day window), spike_ratio = last_7d / NULLIF(baseline*7,0), is_spike = TRUE when spike_ratio > 2.0
   - For spiked rows calls SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', prompt) asking: "In 2 sentences, explain why a financial institution should urgently investigate a spike_ratio of X in event_type Y and what the likely cause could be."
   - Inserts ALL rows; cortex_analysis NULL for non-spiked
   - Logs to AUDIT.ETL_RUN_LOG; IMPORTANT: bind :v_duration not inline DATEDIFF
   - Returns VARIANT with status, rows_inserted, spikes_detected
3. Add as Step 9 in AUDIT.SP_AUTONOMOUS_ETL_AGENT with retry loop; include r_anomalies in RETURN
4. Run CALL AUDIT.SP_AUTONOMOUS_ETL_AGENT(3, NULL)
5. SELECT event_type, spike_ratio, is_spike, cortex_analysis FROM ANALYTICS.RISK_ANOMALY_LOG WHERE is_spike=TRUE ORDER BY spike_ratio DESC"""
        },
        {
            "num":4,"label":"Observability Views (6 views)","tag":"Audit · Health Monitoring · Views","accent":"#fbbf24","bg":"#1a1007",
            "what":"Creates 6 AUDIT schema views covering pipeline health, hourly trend, DQ summary, slowest procs, recent failures, and row throughput trend.",
            "prompt":"""I have FINANCE_ETL_DEMO.AUDIT with ETL_RUN_LOG (run_id, pipeline_name, layer, procedure_name, status, rows_processed, rows_inserted, rows_rejected, error_message, started_at, finished_at, duration_sec) and DATA_QUALITY_LOG (check_id, table_name, check_name, check_type, passed, failed_rows, total_rows, checked_at).

Create these 6 views:

AUDIT.VW_PIPELINE_HEALTH_SUMMARY — last 7 days: procedure_name, layer, total_runs, successful_runs, failed_runs, success_rate_pct, avg_duration_sec, max_duration_sec, last_run_at, last_run_status. ORDER BY layer, procedure_name.

AUDIT.VW_HOURLY_RUN_TREND — last 7 days by DATE_TRUNC('hour',started_at): run_hour, total_runs, successful_runs, failed_runs, avg_duration_sec.

AUDIT.VW_DATA_QUALITY_SUMMARY — all time by table_name + check_type: table_name, check_type, total_checks, passed_checks, failed_checks, pass_rate_pct, avg_failed_rows, last_checked_at.

AUDIT.VW_SLOWEST_PROCEDURES — last 7 days top 10: procedure_name, layer, avg_duration_sec, max_duration_sec, min_duration_sec, total_runs. ORDER BY avg_duration_sec DESC.

AUDIT.VW_RECENT_FAILURES — last 7 days STATUS='FAILED': started_at, procedure_name, layer, error_message, duration_sec. ORDER BY started_at DESC.

AUDIT.VW_ROWS_PROCESSED_TREND — last 7 days by date+layer: run_date, layer, total_rows_inserted, total_runs, avg_rows_per_run. ORDER BY run_date DESC, layer.

Then SELECT * on each view. Then run: SELECT procedure_name, success_rate_pct, avg_duration_sec, last_run_status FROM AUDIT.VW_PIPELINE_HEALTH_SUMMARY ORDER BY layer, procedure_name;"""
        },
        {
            "num":5,"label":"Graceful Pipeline Shutdown","tag":"Task Management · Audit","accent":"#f87171","bg":"#1a0707",
            "what":"Safely suspends the task, verifies state, shows last 5 runs from both Snowflake task history and our own audit log.",
            "prompt":"""In FINANCE_ETL_DEMO:
1. ALTER TASK AUDIT.TASK_ETL_ORCHESTRATOR SUSPEND
2. SHOW TASKS LIKE 'TASK_ETL_ORCHESTRATOR' IN SCHEMA FINANCE_ETL_DEMO.AUDIT
3. SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START=>DATEADD('day',-7,CURRENT_TIMESTAMP()), TASK_NAME=>'TASK_ETL_ORCHESTRATOR')) ORDER BY SCHEDULED_TIME DESC LIMIT 5
4. SELECT run_id, status, started_at, finished_at, duration_sec, error_message FROM AUDIT.ETL_RUN_LOG WHERE procedure_name='SP_AUTONOMOUS_ETL_AGENT' ORDER BY started_at DESC LIMIT 5
5. Confirm task state, schedule, last successful run time, total runs in last 7 days"""
        },
    ]

    for p in COCO_PROMPTS:
        with st.expander(f"Prompt {p['num']} — {p['label']}", expanded=(p["num"]==1)):
            st.markdown(f"""
            <div style="background:{p['bg']};border:1px solid {p['accent']}22;border-left:3px solid {p['accent']};
                        border-radius:8px;padding:14px 18px;margin-bottom:12px">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
                    <span style="font-size:10px;color:{p['accent']};font-weight:700;letter-spacing:0.08em;text-transform:uppercase">{p['tag']}</span>
                    <span style="font-size:10px;color:#3d5066;font-family:IBM Plex Mono,monospace">Prompt {p['num']} / 5</span>
                </div>
                <p style="font-size:13px;color:#7a8fa8;margin:0;line-height:1.6">{p['what']}</p>
            </div>""", unsafe_allow_html=True)
            st.code(p["prompt"], language="text")
            st.markdown(f"""
            <div style="font-size:11px;color:#3d5066;padding:8px 12px;background:#0a0f18;
                        border:1px solid #141e2e;border-radius:6px;margin-top:6px">
                <strong style="color:{p['accent']}">How to run:</strong>
                Snowsight Worksheet → CoCo icon (top-right) → paste → Enter → review SQL → Run
            </div>""", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("""
    <div style="background:#0a0f18;border:1px solid #141e2e;border-radius:10px;padding:20px 24px">
        <div style="font-family:'Syne',sans-serif;font-size:16px;color:#e2eaf4;font-weight:500;margin-bottom:16px">
            CoCo Self-Healing Event — What happened during Prompt 3
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px">
            <div style="border:1px solid #f8717122;border-left:3px solid #f87171;border-radius:8px;padding:14px">
                <div style="font-size:10px;color:#f87171;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;margin-bottom:8px">⚠ The Failure</div>
                <p style="font-size:12px;color:#7a8fa8;line-height:1.7;margin:0">
                    CoCo generated a <strong style="color:#c8d8ec">correlated subquery inside a CTE</strong> — 
                    a pattern Snowflake doesn't support. Pipeline failed with a SQL compilation error on first run.
                </p>
            </div>
            <div style="border:1px solid #4ade8022;border-left:3px solid #4ade80;border-radius:8px;padding:14px">
                <div style="font-size:10px;color:#4ade80;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;margin-bottom:8px">✓ The Recovery</div>
                <div style="font-size:12px;color:#7a8fa8;line-height:1.8">
                    <div>1. CoCo read the Snowflake error message</div>
                    <div>2. Identified the correlated subquery constraint</div>
                    <div>3. Rewrote as 3 flat CTEs with LEFT JOINs</div>
                    <div style="color:#4ade80;margin-top:4px">4. All 9 steps succeeded ✓</div>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ── Footer ─────────────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-top:3rem;padding-top:1rem;border-top:1px solid #141e2e;
            display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
    <span style="font-family:IBM Plex Mono,monospace;font-size:10px;color:#1e2d42">
        Autonomous Finance ETL Agent · v3.0 · Snowflake CoCo Hackathon
    </span>
    <span style="font-family:IBM Plex Mono,monospace;font-size:10px;color:#1e2d42">
        Bronze→Silver→Gold · Cortex AI · 100% Snowflake Native
    </span>
</div>
""", unsafe_allow_html=True)