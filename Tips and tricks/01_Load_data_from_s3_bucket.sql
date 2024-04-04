--
-- LOAD DATA FROM S3 BUCKET TO SNOWFLAKE
--


-- Specify database and schema
USE DATABASE USERS_DB;
USE SCHEMA PESTYL;

-- 1. CREATE TABLE SCHEMA
CREATE OR REPLACE TABLE LOAN_PAYMENT (
    LOAN_ID STRING,
    LOAN_STATUS STRING,
    PRINCIPAL STRING,
    TERMS STRING,
    EFFECTIVE_DATE STRING,
    DUE_DATE STRING,
    PAID_OFF_TIME STRING,
    PAST_DUE_DAYS STRING,
    AGE STRING,
    EDUCATION STRING,
    GENDER STRING
);


-- 2. COPY INTO EMPTY TABLE
COPY INTO LOAN_PAYMENT
    FROM s3://bucketsnowflakes3/Loan_payments_data.csv
    FILE_FORMAT = (
        type = csv
        field_delimiter = ','
        skip_header = 1
    );

-- 3. VALIDATE
SELECT *
FROM LOAN_PAYMENT
LIMIT 10;