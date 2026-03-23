CREATE OR REPLACE PROCEDURE STAGING.SP_LOAD_CUSTOMERS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_inserted  INTEGER       DEFAULT 0;
    v_duration  NUMBER(10,2);
    v_error     VARCHAR;
BEGIN
    INSERT INTO AUDIT.DATA_QUALITY_LOG (table_name, check_name, check_type, passed, failed_rows, total_rows)
    SELECT
        'RAW.CUSTOMERS', 'NULL customer_id check', 'NULL_CHECK',
        (SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) = 0),
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
        COUNT(*)
    FROM RAW.CUSTOMERS;

    MERGE INTO STAGING.CUSTOMERS tgt
    USING (
        SELECT
            customer_id,
            first_name || ' ' || last_name                            AS full_name,
            LOWER(TRIM(email))                                        AS email,
            phone,
            DATEDIFF('year', date_of_birth, CURRENT_DATE())          AS age,
            country, city, credit_score,
            CASE WHEN credit_score >= 750 THEN 'EXCELLENT'
                 WHEN credit_score >= 670 THEN 'GOOD'
                 WHEN credit_score >= 580 THEN 'FAIR'
                 ELSE 'POOR' END                                      AS credit_tier,
            annual_income,
            CASE WHEN annual_income >= 150000 THEN 'HIGH'
                 WHEN annual_income >= 60000  THEN 'MEDIUM'
                 ELSE 'LOW' END                                       AS income_bracket,
            customer_segment, kyc_status,
            (kyc_status = 'VERIFIED')                                 AS is_verified,
            created_at,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS rn
        FROM RAW.CUSTOMERS
        WHERE customer_id IS NOT NULL
    ) src ON (src.rn = 1 AND tgt.customer_id = src.customer_id)
    WHEN MATCHED THEN UPDATE SET
        full_name      = src.full_name,
        credit_tier    = src.credit_tier,
        income_bracket = src.income_bracket,
        kyc_status     = src.kyc_status,
        is_verified    = src.is_verified,
        _etl_loaded_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        customer_id, full_name, email, phone, age, country, city,
        credit_score, credit_tier, annual_income, income_bracket,
        customer_segment, kyc_status, is_verified, created_at
    ) VALUES (
        src.customer_id, src.full_name, src.email, src.phone, src.age,
        src.country, src.city, src.credit_score, src.credit_tier,
        src.annual_income, src.income_bracket, src.customer_segment,
        src.kyc_status, src.is_verified, src.created_at
    );

    v_inserted := SQLROWCOUNT;
    v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);

    INSERT INTO AUDIT.ETL_RUN_LOG
        (pipeline_name, layer, procedure_name, status, rows_inserted, started_at, finished_at, duration_sec)
    VALUES
        ('FINANCE_ETL', 'STAGING', 'SP_LOAD_CUSTOMERS', 'SUCCESS', :v_inserted, :v_start, CURRENT_TIMESTAMP(), :v_duration);

    RETURN OBJECT_CONSTRUCT('status','SUCCESS','rows_inserted',:v_inserted);
EXCEPTION
    WHEN OTHER THEN
        v_error    := SQLERRM;
        v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);
        INSERT INTO AUDIT.ETL_RUN_LOG
            (pipeline_name, layer, procedure_name, status, error_message, started_at, finished_at, duration_sec)
        VALUES
            ('FINANCE_ETL', 'STAGING', 'SP_LOAD_CUSTOMERS', 'FAILED', :v_error, :v_start, CURRENT_TIMESTAMP(), :v_duration);
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',:v_error);
END;
$$;

CREATE OR REPLACE PROCEDURE STAGING.SP_LOAD_ACCOUNTS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_inserted  INTEGER       DEFAULT 0;
    v_duration  NUMBER(10,2);
    v_error     VARCHAR;
BEGIN
    MERGE INTO STAGING.ACCOUNTS tgt
    USING (
        SELECT
            account_id, customer_id, account_type, account_number, currency, balance,
            CASE WHEN balance <  0    THEN 'NEGATIVE'
                 WHEN balance =  0    THEN 'ZERO'
                 WHEN balance < 1000  THEN 'LOW'
                 WHEN balance < 50000 THEN 'MEDIUM'
                 ELSE 'HIGH' END      AS balance_bucket,
            credit_limit, interest_rate, status,
            (status = 'ACTIVE')       AS is_active,
            opened_date,
            DATEDIFF('day', opened_date, CURRENT_DATE()) AS account_age_days,
            ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY created_at DESC) AS rn
        FROM RAW.ACCOUNTS
        WHERE account_id IS NOT NULL
    ) src ON (src.rn = 1 AND tgt.account_id = src.account_id)
    WHEN MATCHED THEN UPDATE SET
        balance        = src.balance,
        balance_bucket = src.balance_bucket,
        status         = src.status,
        is_active      = src.is_active,
        _etl_loaded_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        account_id, customer_id, account_type, account_number, currency,
        balance, balance_bucket, credit_limit, interest_rate, status,
        is_active, opened_date, account_age_days
    ) VALUES (
        src.account_id, src.customer_id, src.account_type, src.account_number,
        src.currency, src.balance, src.balance_bucket, src.credit_limit,
        src.interest_rate, src.status, src.is_active, src.opened_date,
        src.account_age_days
    );

    v_inserted := SQLROWCOUNT;
    v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);

    INSERT INTO AUDIT.ETL_RUN_LOG
        (pipeline_name, layer, procedure_name, status, rows_inserted, started_at, finished_at, duration_sec)
    VALUES
        ('FINANCE_ETL', 'STAGING', 'SP_LOAD_ACCOUNTS', 'SUCCESS', :v_inserted, :v_start, CURRENT_TIMESTAMP(), :v_duration);

    RETURN OBJECT_CONSTRUCT('status','SUCCESS','rows_inserted',:v_inserted);
EXCEPTION
    WHEN OTHER THEN
        v_error    := SQLERRM;
        v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);
        INSERT INTO AUDIT.ETL_RUN_LOG
            (pipeline_name, layer, procedure_name, status, error_message, started_at, finished_at, duration_sec)
        VALUES
            ('FINANCE_ETL', 'STAGING', 'SP_LOAD_ACCOUNTS', 'FAILED', :v_error, :v_start, CURRENT_TIMESTAMP(), :v_duration);
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',:v_error);
END;
$$;

CREATE OR REPLACE PROCEDURE STAGING.SP_LOAD_TRANSACTIONS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_inserted  INTEGER       DEFAULT 0;
    v_duration  NUMBER(10,2);
    v_error     VARCHAR;
BEGIN
    MERGE INTO STAGING.TRANSACTIONS tgt
    USING (
        SELECT
            t.transaction_id, t.account_id, a.customer_id,
            t.transaction_type, t.amount,
            t.amount                                                  AS amount_usd,
            t.currency, t.merchant_name, t.merchant_category,
            t.channel, t.status,
            (t.status = 'COMPLETED')                                  AS is_successful,
            (t.amount > 5000)                                         AS is_large_txn,
            t.reference_code, t.transaction_dt,
            YEAR(t.transaction_dt)                                    AS txn_year,
            MONTH(t.transaction_dt)                                   AS txn_month,
            DAY(t.transaction_dt)                                     AS txn_day,
            HOUR(t.transaction_dt)                                    AS txn_hour,
            DAYNAME(t.transaction_dt)                                 AS txn_weekday,
            ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY t.created_at DESC) AS rn
        FROM RAW.TRANSACTIONS t
        LEFT JOIN STAGING.ACCOUNTS a ON t.account_id = a.account_id
        WHERE t.transaction_id IS NOT NULL AND t.amount > 0
    ) src ON (src.rn = 1 AND tgt.transaction_id = src.transaction_id)
    WHEN MATCHED THEN UPDATE SET
        status         = src.status,
        is_successful  = src.is_successful,
        _etl_loaded_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        transaction_id, account_id, customer_id, transaction_type,
        amount, amount_usd, currency, merchant_name, merchant_category,
        channel, status, is_successful, is_large_txn, reference_code,
        transaction_dt, txn_year, txn_month, txn_day, txn_hour, txn_weekday
    ) VALUES (
        src.transaction_id, src.account_id, src.customer_id, src.transaction_type,
        src.amount, src.amount_usd, src.currency, src.merchant_name,
        src.merchant_category, src.channel, src.status, src.is_successful,
        src.is_large_txn, src.reference_code, src.transaction_dt,
        src.txn_year, src.txn_month, src.txn_day, src.txn_hour, src.txn_weekday
    );

    v_inserted := SQLROWCOUNT;
    v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);

    INSERT INTO AUDIT.ETL_RUN_LOG
        (pipeline_name, layer, procedure_name, status, rows_inserted, started_at, finished_at, duration_sec)
    VALUES
        ('FINANCE_ETL', 'STAGING', 'SP_LOAD_TRANSACTIONS', 'SUCCESS', :v_inserted, :v_start, CURRENT_TIMESTAMP(), :v_duration);

    RETURN OBJECT_CONSTRUCT('status','SUCCESS','rows_inserted',:v_inserted);
EXCEPTION
    WHEN OTHER THEN
        v_error    := SQLERRM;
        v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);
        INSERT INTO AUDIT.ETL_RUN_LOG
            (pipeline_name, layer, procedure_name, status, error_message, started_at, finished_at, duration_sec)
        VALUES
            ('FINANCE_ETL', 'STAGING', 'SP_LOAD_TRANSACTIONS', 'FAILED', :v_error, :v_start, CURRENT_TIMESTAMP(), :v_duration);
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',:v_error);
END;
$$;

CREATE OR REPLACE PROCEDURE STAGING.SP_LOAD_RISK_EVENTS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_inserted  INTEGER       DEFAULT 0;
    v_duration  NUMBER(10,2);
    v_error     VARCHAR;
BEGIN
    MERGE INTO STAGING.RISK_EVENTS tgt
    USING (
        SELECT
            r.event_id, r.account_id, a.customer_id, r.transaction_id,
            r.event_type, r.severity,
            CASE r.severity WHEN 'LOW'      THEN 1
                            WHEN 'MEDIUM'   THEN 2
                            WHEN 'HIGH'     THEN 3
                            WHEN 'CRITICAL' THEN 4
                            ELSE 0 END       AS severity_rank,
            r.score, r.flagged_by, r.resolved, r.event_dt,
            ROW_NUMBER() OVER (PARTITION BY r.event_id ORDER BY r.event_dt DESC) AS rn
        FROM RAW.RISK_EVENTS r
        LEFT JOIN STAGING.ACCOUNTS a ON r.account_id = a.account_id
        WHERE r.event_id IS NOT NULL
    ) src ON (src.rn = 1 AND tgt.event_id = src.event_id)
    WHEN MATCHED THEN UPDATE SET
        resolved       = src.resolved,
        _etl_loaded_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        event_id, account_id, customer_id, transaction_id,
        event_type, severity, severity_rank, score, flagged_by, resolved, event_dt
    ) VALUES (
        src.event_id, src.account_id, src.customer_id, src.transaction_id,
        src.event_type, src.severity, src.severity_rank, src.score,
        src.flagged_by, src.resolved, src.event_dt
    );

    v_inserted := SQLROWCOUNT;
    v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);

    INSERT INTO AUDIT.ETL_RUN_LOG
        (pipeline_name, layer, procedure_name, status, rows_inserted, started_at, finished_at, duration_sec)
    VALUES
        ('FINANCE_ETL', 'STAGING', 'SP_LOAD_RISK_EVENTS', 'SUCCESS', :v_inserted, :v_start, CURRENT_TIMESTAMP(), :v_duration);

    RETURN OBJECT_CONSTRUCT('status','SUCCESS','rows_inserted',:v_inserted);
EXCEPTION
    WHEN OTHER THEN
        v_error    := SQLERRM;
        v_duration := ROUND(DATEDIFF('millisecond', :v_start, CURRENT_TIMESTAMP()) / 1000.0, 2);
        INSERT INTO AUDIT.ETL_RUN_LOG
            (pipeline_name, layer, procedure_name, status, error_message, started_at, finished_at, duration_sec)
        VALUES
            ('FINANCE_ETL', 'STAGING', 'SP_LOAD_RISK_EVENTS', 'FAILED', :v_error, :v_start, CURRENT_TIMESTAMP(), :v_duration);
        RETURN OBJECT_CONSTRUCT('status','FAILED','error',:v_error);
END;
$$;
