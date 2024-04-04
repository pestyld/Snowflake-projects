USE DATABASE USERS_DB;
USE SCHEMA PESTYL;


CREATE OR REPLACE TABLE customer_test
    (
        column1 int
    );

-- Default retention_time is 1
SHOW TABLES LIKE '%CUSTOMER%';

-- Set retention time to 2
CREATE OR REPLACE TABLE customer_test
    (
        column1 int
    )
    DATA_RETENTION_TIME_IN_DAYS = 2;


-- Disable retention time
ALTER TABLE customer_test
    SET DATA_RETENTION_TIME_IN_DAYS = 0;
