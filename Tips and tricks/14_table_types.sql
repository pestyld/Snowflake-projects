-- TABLE TYPES
    -- Permanent
        -- Time travel retention period (0 - 90 days - Enterprise is required             for more than 1 day)
        -- Fail safe
        -- Production level tables
    -- Transient
        -- Time travel retention period (0-1 day only)
        -- No fail safe
        -- Saves data storage costs
        -- Testing purposes typically, don't care if you lose it. Not                     production level data.
        -- Small storage size
    -- Temporary
        -- Time travel retention period (0-1 day only)
        -- No fail safe
        -- Different from transiet because they are only used for the session.            Gone after session. Only valid for user session
        -- Non permanent data. Usually for tests


USE DATABASE USERS_DB;
USE SCHEMA PESTYL;


-- Permanent table
CREATE OR REPLACE TABLE customers(
    ID int,
    first_name string,
    last_name string,
    email string,
    gender string,
    job string,
    phone string
);

SHOW TABLES IN PESTYL;



-- Transient table
CREATE OR REPLACE TRANSIENT TABLE customers(
    ID int,
    first_name string,
    last_name string,
    email string,
    gender string,
    job string,
    phone string
);

SHOW TABLES IN PESTYL;


-- Temporary table (only available in this worksheet/session)
CREATE OR REPLACE TEMPORARY TABLE customers (
    ID int,
    first_name string,
    last_name string,
    email string,
    gender string,
    job string,
    phone string
);

SHOW TABLES IN PESTYL;



-- View table metrics
SELECT * 
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;