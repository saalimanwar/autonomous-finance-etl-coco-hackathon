USE SCHEMA RAW;

CREATE OR REPLACE TABLE RAW.CUSTOMERS (
    customer_id       VARCHAR(20),
    first_name        VARCHAR(50),
    last_name         VARCHAR(50),
    email             VARCHAR(100),
    phone             VARCHAR(20),
    date_of_birth     DATE,
    country           VARCHAR(50),
    city              VARCHAR(50),
    credit_score      NUMBER(3,0),
    annual_income     NUMBER(12,2),
    customer_segment  VARCHAR(20),
    kyc_status        VARCHAR(20),
    created_at        TIMESTAMP_NTZ,
    _load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.ACCOUNTS (
    account_id        VARCHAR(20),
    customer_id       VARCHAR(20),
    account_type      VARCHAR(30),
    account_number    VARCHAR(30),
    currency          VARCHAR(3),
    balance           NUMBER(18,2),
    credit_limit      NUMBER(18,2),
    interest_rate     NUMBER(6,4),
    status            VARCHAR(20),
    opened_date       DATE,
    closed_date       DATE,
    created_at        TIMESTAMP_NTZ,
    _load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.TRANSACTIONS (
    transaction_id    VARCHAR(30),
    account_id        VARCHAR(20),
    transaction_type  VARCHAR(30),
    amount            NUMBER(18,2),
    currency          VARCHAR(3),
    merchant_name     VARCHAR(100),
    merchant_category VARCHAR(50),
    channel           VARCHAR(20),
    status            VARCHAR(20),
    reference_code    VARCHAR(40),
    description       VARCHAR(200),
    transaction_dt    TIMESTAMP_NTZ,
    created_at        TIMESTAMP_NTZ,
    _load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.RISK_EVENTS (
    event_id          VARCHAR(30),
    account_id        VARCHAR(20),
    transaction_id    VARCHAR(30),
    event_type        VARCHAR(50),
    severity          VARCHAR(10),
    score             NUMBER(5,2),
    description       VARCHAR(500),
    flagged_by        VARCHAR(50),
    resolved          BOOLEAN,
    event_dt          TIMESTAMP_NTZ,
    _load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
