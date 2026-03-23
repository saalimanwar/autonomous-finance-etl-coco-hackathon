USE SCHEMA AUDIT;

CREATE OR REPLACE TABLE AUDIT.ETL_RUN_LOG (
    run_id          VARCHAR(40)    DEFAULT 'RUN-' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD-HH24MISS'),
    pipeline_name   VARCHAR(100),
    layer           VARCHAR(20),
    procedure_name  VARCHAR(100),
    status          VARCHAR(20),
    rows_processed  NUMBER(18,0),
    rows_inserted   NUMBER(18,0),
    rows_rejected   NUMBER(18,0),
    error_message   VARCHAR(2000),
    started_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    finished_at     TIMESTAMP_NTZ,
    duration_sec    NUMBER(10,2)
);

CREATE OR REPLACE TABLE AUDIT.DATA_QUALITY_LOG (
    check_id        VARCHAR(40)    DEFAULT 'DQ-' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD-HH24MISS'),
    table_name      VARCHAR(100),
    check_name      VARCHAR(100),
    check_type      VARCHAR(50),
    passed          BOOLEAN,
    failed_rows     NUMBER(18,0),
    total_rows      NUMBER(18,0),
    checked_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
