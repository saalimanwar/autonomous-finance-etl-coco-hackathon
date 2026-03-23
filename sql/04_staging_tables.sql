USE SCHEMA STAGING;

CREATE OR REPLACE TABLE STAGING.CUSTOMERS (
    customer_id        VARCHAR(20)   NOT NULL PRIMARY KEY,
    full_name          VARCHAR(100),
    email              VARCHAR(100),
    phone              VARCHAR(20),
    age                NUMBER(3,0),
    country            VARCHAR(50),
    city               VARCHAR(50),
    credit_score       NUMBER(3,0),
    credit_tier        VARCHAR(20),
    annual_income      NUMBER(12,2),
    income_bracket     VARCHAR(20),
    customer_segment   VARCHAR(20),
    kyc_status         VARCHAR(20),
    is_verified        BOOLEAN,
    created_at         TIMESTAMP_NTZ,
    _etl_loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGING.ACCOUNTS (
    account_id         VARCHAR(20)   NOT NULL PRIMARY KEY,
    customer_id        VARCHAR(20),
    account_type       VARCHAR(30),
    account_number     VARCHAR(30),
    currency           VARCHAR(3),
    balance            NUMBER(18,2),
    balance_bucket     VARCHAR(20),
    credit_limit       NUMBER(18,2),
    interest_rate      NUMBER(6,4),
    status             VARCHAR(20),
    is_active          BOOLEAN,
    opened_date        DATE,
    account_age_days   NUMBER(10,0),
    _etl_loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGING.TRANSACTIONS (
    transaction_id     VARCHAR(30)   NOT NULL PRIMARY KEY,
    account_id         VARCHAR(20),
    customer_id        VARCHAR(20),
    transaction_type   VARCHAR(30),
    amount             NUMBER(18,2),
    amount_usd         NUMBER(18,2),
    currency           VARCHAR(3),
    merchant_name      VARCHAR(100),
    merchant_category  VARCHAR(50),
    channel            VARCHAR(20),
    status             VARCHAR(20),
    is_successful      BOOLEAN,
    is_large_txn       BOOLEAN,
    reference_code     VARCHAR(40),
    transaction_dt     TIMESTAMP_NTZ,
    txn_year           NUMBER(4,0),
    txn_month          NUMBER(2,0),
    txn_day            NUMBER(2,0),
    txn_hour           NUMBER(2,0),
    txn_weekday        VARCHAR(10),
    _etl_loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGING.RISK_EVENTS (
    event_id           VARCHAR(30)   NOT NULL PRIMARY KEY,
    account_id         VARCHAR(20),
    customer_id        VARCHAR(20),
    transaction_id     VARCHAR(30),
    event_type         VARCHAR(50),
    severity           VARCHAR(10),
    severity_rank      NUMBER(1,0),
    score              NUMBER(5,2),
    flagged_by         VARCHAR(50),
    resolved           BOOLEAN,
    event_dt           TIMESTAMP_NTZ,
    _etl_loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
