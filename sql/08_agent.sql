CREATE OR REPLACE PROCEDURE AUDIT.SP_AUTONOMOUS_ETL_AGENT(
    MAX_RETRIES   INTEGER DEFAULT 3,
    NOTIFY_EMAIL  VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_attempt       INTEGER       DEFAULT 0;
    v_result        VARIANT;
    v_status        VARCHAR;
    v_pipeline_log  ARRAY         DEFAULT ARRAY_CONSTRUCT();
    v_error         VARCHAR;
    v_duration      NUMBER(10,2);

    r_customers     VARIANT;
    r_accounts      VARIANT;
    r_transactions  VARIANT;
    r_risk          VARIANT;
    r_daily         VARIANT;
    r_health        VARIANT;
    r_risk_dash     VARIANT;

BEGIN
    -- Step 1: Staging — Customers
    v_attempt := 0;
    LOOP
        v_attempt := v_attempt + 1;
        CALL STAGING.SP_LOAD_CUSTOMERS() INTO r_customers;
        IF (r_customers:status::VARCHAR = 'SUCCESS') THEN BREAK; END IF;
        IF (v_attempt >= MAX_RETRIES) THEN
            RETURN OBJECT_CONSTRUCT(
                'agent_status','FAILED',
                'failed_step','SP_LOAD_CUSTOMERS',
                'attempts', v_attempt,
                'error', r_customers:error::VARCHAR
            );
        END IF;
    END LOOP;

    -- Step 2: Staging — Accounts
    v_attempt := 0;
    LOOP
        v_attempt := v_attempt + 1;
        CALL STAGING.SP_LOAD_ACCOUNTS() INTO r_accounts;
        IF (r_accounts:status::VARCHAR = 'SUCCESS') THEN BREAK; END IF;
        IF (v_attempt >= MAX_RETRIES) THEN
            RETURN OBJECT_CONSTRUCT('agent_status','FAILED','failed_step','SP_LOAD_ACCOUNTS','error',r_accounts:error::VARCHAR);
        END IF;
    END LOOP;

    -- Step 3: Staging — Transactions
    v_attempt := 0;
    LOOP
        v_attempt := v_attempt + 1;
        CALL STAGING.SP_LOAD_TRANSACTIONS() INTO r_transactions;
        IF (r_transactions:status::VARCHAR = 'SUCCESS') THEN BREAK; END IF;
        IF (v_attempt >= MAX_RETRIES) THEN
            RETURN OBJECT_CONSTRUCT('agent_status','FAILED','failed_step','SP_LOAD_TRANSACTIONS','error',r_transactions:error::VARCHAR);
        END IF;
    END LOOP;

    -- Step 4: Staging — Risk Events
    v_attempt := 0;
    LOOP
        v_attempt := v_attempt + 1;
        CALL STAGING.SP_LOAD_RISK_EVENTS() INTO r_risk;
        IF (r_risk:status::VARCHAR = 'SUCCESS') THEN BREAK; END IF;
        IF (v_attempt >= MAX_RETRIES) THEN
            RETURN OBJECT_CONSTRUCT('agent_status','FAILED','failed_step','SP_LOAD_RISK_EVENTS','error',r_risk:error::VARCHAR);
        END IF;
    END LOOP;

    -- Step 5: Analytics — Daily Summary
    v_attempt := 0;
    LOOP
        v_attempt := v_attempt + 1;
        CALL ANALYTICS.SP_BUILD_DAILY_TXN_SUMMARY() INTO r_daily;
        IF (r_daily:status::VARCHAR = 'SUCCESS') THEN BREAK; END IF;
        IF (v_attempt >= MAX_RETRIES) THEN
            RETURN OBJECT_CONSTRUCT('agent_status','FAILED','failed_step','SP_BUILD_DAILY_TXN_SUMMARY','error',r_daily:error::VARCHAR);
        END IF;
    END LOOP;

    -- Step 6: Analytics — Customer Health
    v_attempt := 0;
    LOOP
        v_attempt := v_attempt + 1;
        CALL ANALYTICS.SP_BUILD_CUSTOMER_HEALTH() INTO r_health;
        IF (r_health:status::VARCHAR = 'SUCCESS') THEN BREAK; END IF;
        IF (v_attempt >= MAX_RETRIES) THEN
            RETURN OBJECT_CONSTRUCT('agent_status','FAILED','failed_step','SP_BUILD_CUSTOMER_HEALTH','error',r_health:error::VARCHAR);
        END IF;
    END LOOP;

    -- Step 7: Analytics — Risk Dashboard
    v_attempt := 0;
    LOOP
        v_attempt := v_attempt + 1;
        CALL ANALYTICS.SP_BUILD_RISK_DASHBOARD() INTO r_risk_dash;
        IF (r_risk_dash:status::VARCHAR = 'SUCCESS') THEN BREAK; END IF;
        IF (v_attempt >= MAX_RETRIES) THEN
            RETURN OBJECT_CONSTRUCT('agent_status','FAILED','failed_step','SP_BUILD_RISK_DASHBOARD','error',r_risk_dash:error::VARCHAR);
        END IF;
    END LOOP;

    -- Final audit entry
    v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);

    INSERT INTO AUDIT.ETL_RUN_LOG
        (pipeline_name, layer, procedure_name, status, started_at, finished_at, duration_sec)
    VALUES
        ('FINANCE_ETL', 'ALL', 'SP_AUTONOMOUS_ETL_AGENT', 'SUCCESS',
         :v_start, CURRENT_TIMESTAMP(), :v_duration);

    RETURN OBJECT_CONSTRUCT(
        'agent_status',       'SUCCESS',
        'total_duration_sec', :v_duration,
        'steps', OBJECT_CONSTRUCT(
            'customers',     r_customers,
            'accounts',      r_accounts,
            'transactions',  r_transactions,
            'risk_events',   r_risk,
            'daily_summary', r_daily,
            'cust_health',   r_health,
            'risk_dashboard',r_risk_dash
        )
    );

EXCEPTION
    WHEN OTHER THEN
        v_error    := SQLERRM;
        v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);
        INSERT INTO AUDIT.ETL_RUN_LOG
            (pipeline_name, layer, procedure_name, status, error_message, started_at, finished_at, duration_sec)
        VALUES
            ('FINANCE_ETL', 'ALL', 'SP_AUTONOMOUS_ETL_AGENT', 'FAILED',
             :v_error, :v_start, CURRENT_TIMESTAMP(), :v_duration);
        RETURN OBJECT_CONSTRUCT('agent_status','FAILED','error',:v_error);
END;
$$;
