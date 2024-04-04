--
-- FORCE COPY INTO
-- the FORCE option in Snowflake's data-loading process, which can significantly impact query execution 
-- and data loading. Understand when and how to use FORCE effectively, ensuring smooth data transfers and processing.
--



-- Specify database and schema
USE DATABASE USERS_DB;
USE SCHEMA PESTYL;

-- Create empTy table
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
);

CREATE OR REPLACE STAGE aws_stage_copy
    URL = 's3://snowflakebucket-copyoption/size/';

LIST @aws_stage_copy

COPY INTO ORDERS
    FROM @aws_stage_copy
    PATTERN = '.*Order.*'
    FILE_FORMAT = (type = csv skip_header=1 field_delimiter=',');

SELECT * FROM ORDERS LIMIT 10;

-- 3,000 rows
SELECT count(*) FROM orders;


--
-- LOAD DATA AGAIN
--

-- First load returns an error
COPY INTO ORDERS
    FROM @aws_stage_copy
    PATTERN = '.*Order.*'
    FILE_FORMAT = (type = csv skip_header=1 field_delimiter=',');


-- FORCE another loaded

COPY INTO ORDERS
    FROM @aws_stage_copy
    PATTERN = '.*Order.*'
    FILE_FORMAT = (type = csv skip_header=1 field_delimiter=',')
    FORCE = TRUE;

-- 6,000 rows
SELECT count(*) FROM orders;
