-- Specify database and schema
USE DATABASE USERS_DB;
USE SCHEMA PESTYL;


-- LOAD DATA

-- 1. CREATE TABLE SCHEMA
CREATE OR REPLACE TABLE LOAN_PAYMENT (
    LOAN_ID STRING,
    LOAN_STATUS STRING,
    PRINCIPAL STRING,
    TERMS STRING,
    EFFECTIVE_DATE DATE,
    DUE_DATE DATE,
    PAID_OFF_TIME STRING,
    PAST_DUE_DAYS STRING,
    AGE STRING,
    EDUCATION STRING,
    GENDER STRING
);


COPY INTO LOAN_PAYMENT
    FROM s3://bucketsnowflakes3/Loan_payments_data.csv
    FILE_FORMAT = (type = csv field_delimiter = ',' skip_header = 1);

SELECT * FROM LOAN_PAYMENT LIMIT 10;

DESCRIBE TABLE LOAN_PAYMENT;
