CREATE OR REPLACE PROCEDURE ANALYTICS.SP_BUILD_DAILY_TXN_SUMMARY()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_inserted INTEGER       DEFAULT 0;
    v_duration NUMBER(10,2);
    v_error    VARCHAR;
BEGIN
    TRUNCATE TABLE ANALYTICS.DAILY_TXN_SUMMARY;

    INSERT INTO ANALYTICS.DAILY_TXN_SUMMARY (
        summary_date, merchant_category, channel, transaction_type,
        total_transactions, successful_txns, failed_txns,
        total_amount_usd, avg_amount_usd, max_amount_usd,
        large_txn_count, success_rate_pct
    )
    SELECT
        transaction_dt::DATE,
        merchant_category, channel, transaction_type,
        COUNT(*),
        SUM(CASE WHEN is_successful THEN 1 ELSE 0 END),
        SUM(CASE WHEN NOT is_successful THEN 1 ELSE 0 END),
        ROUND(SUM(amount_usd), 2),
        ROUND(AVG(amount_usd), 2),
        ROUND(MAX(amount_usd), 2),
        SUM(CASE WHEN is_large_txn THEN 1 ELSE 0 END),
        ROUND(SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*),0), 2)
    FROM STAGING.TRANSACTIONS
    GROUP BY 1,2,3,4;

    v_inserted := SQLROWCOUNT;
    v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);

    INSERT INTO AUDIT.ETL_RUN_LOG
        (pipeline_name, layer, procedure_name, status, rows_inserted, started_at, finished_at, duration_sec)
    VALUES
        ('FINANCE_ETL', 'ANALYTICS', 'SP_BUILD_DAILY_TXN_SUMMARY', 'SUCCESS', :v_inserted, :v_start, CURRENT_TIMESTAMP(), :v_duration);

    RETURN OBJECT_CONSTRUCT('status','SUCCESS','rows_inserted',:v_inserted);
EXCEPTION
    WHEN OTHER THEN
        v_error    := SQLERRM;
        v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);
        INSERT INTO AUDIT.ETL_RUN_LOG
            (pipeline_name, layer, procedure_name, status, error_message, started_at, finished_at, duration_sec)
        VALUES
            ('FINANCE_ETL', 'ANALYTICS', 'SP_BUILD_DAILY_TXN_SUMMARY', 'FAILED', :v_error, :v_start, CURRENT_TIMESTAMP(), :v_duration);
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',:v_error);
END;
$$;

CREATE OR REPLACE PROCEDURE ANALYTICS.SP_BUILD_CUSTOMER_HEALTH()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_inserted INTEGER       DEFAULT 0;
    v_duration NUMBER(10,2);
    v_error    VARCHAR;
BEGIN
    TRUNCATE TABLE ANALYTICS.CUSTOMER_FINANCIAL_HEALTH;

    INSERT INTO ANALYTICS.CUSTOMER_FINANCIAL_HEALTH (
        customer_id, full_name, credit_tier, income_bracket, customer_segment,
        total_accounts, active_accounts, total_balance_usd, avg_balance_usd,
        total_txn_count, total_txn_volume_usd, avg_txn_amount_usd, last_txn_date,
        risk_event_count, critical_risk_count, risk_score_avg, customer_risk_tier
    )
    SELECT
        c.customer_id, c.full_name, c.credit_tier, c.income_bracket, c.customer_segment,
        COUNT(DISTINCT a.account_id),
        COUNT(DISTINCT CASE WHEN a.is_active THEN a.account_id END),
        ROUND(SUM(a.balance), 2),
        ROUND(AVG(a.balance), 2),
        COUNT(t.transaction_id),
        ROUND(SUM(t.amount_usd), 2),
        ROUND(AVG(t.amount_usd), 2),
        MAX(t.transaction_dt)::DATE,
        COUNT(r.event_id),
        SUM(CASE WHEN r.severity = 'CRITICAL' THEN 1 ELSE 0 END),
        ROUND(AVG(r.score), 2),
        CASE WHEN SUM(CASE WHEN r.severity = 'CRITICAL' THEN 1 ELSE 0 END) > 0
                  OR AVG(r.score) > 75                                THEN 'HIGH_RISK'
             WHEN COUNT(r.event_id) > 3 OR AVG(r.score) > 50         THEN 'MEDIUM_RISK'
             ELSE 'LOW_RISK' END
    FROM STAGING.CUSTOMERS c
    LEFT JOIN STAGING.ACCOUNTS a     ON c.customer_id = a.customer_id
    LEFT JOIN STAGING.TRANSACTIONS t ON a.account_id  = t.account_id
    LEFT JOIN STAGING.RISK_EVENTS r  ON c.customer_id = r.customer_id
    GROUP BY c.customer_id, c.full_name, c.credit_tier, c.income_bracket, c.customer_segment;

    v_inserted := SQLROWCOUNT;
    v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);

    INSERT INTO AUDIT.ETL_RUN_LOG
        (pipeline_name, layer, procedure_name, status, rows_inserted, started_at, finished_at, duration_sec)
    VALUES
        ('FINANCE_ETL', 'ANALYTICS', 'SP_BUILD_CUSTOMER_HEALTH', 'SUCCESS', :v_inserted, :v_start, CURRENT_TIMESTAMP(), :v_duration);

    RETURN OBJECT_CONSTRUCT('status','SUCCESS','rows_inserted',:v_inserted);
EXCEPTION
    WHEN OTHER THEN
        v_error    := SQLERRM;
        v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);
        INSERT INTO AUDIT.ETL_RUN_LOG
            (pipeline_name, layer, procedure_name, status, error_message, started_at, finished_at, duration_sec)
        VALUES
            ('FINANCE_ETL', 'ANALYTICS', 'SP_BUILD_CUSTOMER_HEALTH', 'FAILED', :v_error, :v_start, CURRENT_TIMESTAMP(), :v_duration);
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',:v_error);
END;
$$;

CREATE OR REPLACE PROCEDURE ANALYTICS.SP_BUILD_RISK_DASHBOARD()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_inserted INTEGER       DEFAULT 0;
    v_duration NUMBER(10,2);
    v_error    VARCHAR;
BEGIN
    TRUNCATE TABLE ANALYTICS.RISK_DASHBOARD;

    INSERT INTO ANALYTICS.RISK_DASHBOARD (
        report_date, event_type, severity, flagged_by,
        total_events, resolved_events, unresolved_events,
        avg_risk_score, max_risk_score, resolution_rate_pct
    )
    SELECT
        event_dt::DATE,
        event_type, severity, flagged_by,
        COUNT(*),
        SUM(CASE WHEN resolved THEN 1 ELSE 0 END),
        SUM(CASE WHEN NOT resolved THEN 1 ELSE 0 END),
        ROUND(AVG(score), 2),
        ROUND(MAX(score), 2),
        ROUND(SUM(CASE WHEN resolved THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*),0) * 100, 2)
    FROM STAGING.RISK_EVENTS
    GROUP BY 1,2,3,4;

    v_inserted := SQLROWCOUNT;
    v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);

    INSERT INTO AUDIT.ETL_RUN_LOG
        (pipeline_name, layer, procedure_name, status, rows_inserted, started_at, finished_at, duration_sec)
    VALUES
        ('FINANCE_ETL', 'ANALYTICS', 'SP_BUILD_RISK_DASHBOARD', 'SUCCESS', :v_inserted, :v_start, CURRENT_TIMESTAMP(), :v_duration);

    RETURN OBJECT_CONSTRUCT('status','SUCCESS','rows_inserted',:v_inserted);
EXCEPTION
    WHEN OTHER THEN
        v_error    := SQLERRM;
        v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);
        INSERT INTO AUDIT.ETL_RUN_LOG
            (pipeline_name, layer, procedure_name, status, error_message, started_at, finished_at, duration_sec)
        VALUES
            ('FINANCE_ETL', 'ANALYTICS', 'SP_BUILD_RISK_DASHBOARD', 'FAILED', :v_error, :v_start, CURRENT_TIMESTAMP(), :v_duration);
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',:v_error);
END;
$$;
