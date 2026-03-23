USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE ANALYTICS.DAILY_TXN_SUMMARY (
    summary_date          DATE,
    merchant_category     VARCHAR(50),
    channel               VARCHAR(20),
    transaction_type      VARCHAR(30),
    total_transactions    NUMBER(18,0),
    successful_txns       NUMBER(18,0),
    failed_txns           NUMBER(18,0),
    total_amount_usd      NUMBER(18,2),
    avg_amount_usd        NUMBER(18,2),
    max_amount_usd        NUMBER(18,2),
    large_txn_count       NUMBER(18,0),
    success_rate_pct      NUMBER(6,2),
    _etl_loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE ANALYTICS.CUSTOMER_FINANCIAL_HEALTH (
    customer_id           VARCHAR(20),
    full_name             VARCHAR(100),
    credit_tier           VARCHAR(20),
    income_bracket        VARCHAR(20),
    customer_segment      VARCHAR(20),
    total_accounts        NUMBER(5,0),
    active_accounts       NUMBER(5,0),
    total_balance_usd     NUMBER(18,2),
    avg_balance_usd       NUMBER(18,2),
    total_txn_count       NUMBER(18,0),
    total_txn_volume_usd  NUMBER(18,2),
    avg_txn_amount_usd    NUMBER(18,2),
    last_txn_date         DATE,
    risk_event_count      NUMBER(10,0),
    critical_risk_count   NUMBER(10,0),
    risk_score_avg        NUMBER(6,2),
    customer_risk_tier    VARCHAR(20),
    _etl_loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE ANALYTICS.RISK_DASHBOARD (
    report_date           DATE,
    event_type            VARCHAR(50),
    severity              VARCHAR(10),
    flagged_by            VARCHAR(50),
    total_events          NUMBER(18,0),
    resolved_events       NUMBER(18,0),
    unresolved_events     NUMBER(18,0),
    avg_risk_score        NUMBER(6,2),
    max_risk_score        NUMBER(6,2),
    resolution_rate_pct   NUMBER(6,2),
    _etl_loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
