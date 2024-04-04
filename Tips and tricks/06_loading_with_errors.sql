--
-- USING ON_ERROR WITH COPY INTO
-- master handling errors during data loading with the ON_ERROR option in Snowflake. You will learn to identify, manage, 
-- and resolve errors effectively, ensuring data integrity and reliable data loading practices.
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

CREATE OR REPLACE STAGE aws_stage_errorex
    URL = 's3://bucketsnowflakes4';

LIST @aws_stage_errorex;


--
-- 1. ERROR HANDLING USING ON_ERROR=CONTINUE
-- Loads all valid data. 
-- 

-- Try and load. Returns error
COPY INTO ORDERS
    FROM @aws_stage_errorex
    files=('OrderDetails_error.csv')
    file_format=(type=csv field_delimiter=',' skip_header=1);

-- Load same file with continue 
-- Results - partially loaded. 2 rows not loaded
COPY INTO ORDERS
    FROM @aws_stage_errorex
    files=('OrderDetails_error.csv')
    file_format=(type=csv field_delimiter=',' skip_header=1)
    on_error = 'CONTINUE';

SELECT * FROM ORDERS LIMIT 10;


--
-- 2. DEFAULT OPTION is ON_ERROR='ABORT_STARTMENT'
-- 

-- Set table back to no rows
TRUNCATE TABLE ORDERS;

-- Try and load. Returns error
COPY INTO ORDERS
    FROM @aws_stage_errorex
    files=('OrderDetails_error.csv')
    file_format=(type=csv field_delimiter=',' skip_header=1)
    ON_ERROR = 'ABORT_STATEMENT';


--
-- 3. USE SKIP_FILE TO SKIP A BAD FILE
-- 

-- Set table back to no rows
TRUNCATE TABLE ORDERS;

LIST @aws_stage_errorex;

-- Skips any file with a bad import
COPY INTO ORDERS
    FROM @aws_stage_errorex
    FILES = ('OrderDetails_error.csv', 'OrderDetails_error2.csv')
    FILE_FORMAT = (type = csv skip_header=1 field_delimiter=',')
    ON_ERROR = 'SKIP_FILE';

SELECT * FROM ORDERS LIMIT 10;



--
-- 4. USE SKIP_FILE_N TO Skip a file when the number of error rows found in the file is equal to or exceeds the specified number.
--    2 errors are in OrderDetails_error.csv. Will be partially loaded
-- 

-- Set table back to no rows
TRUNCATE TABLE ORDERS;

-- Skips any file with a bad import
COPY INTO ORDERS
    FROM @aws_stage_errorex
    FILES = ('OrderDetails_error.csv', 'OrderDetails_error2.csv')
    FILE_FORMAT = (type = csv skip_header=1 field_delimiter=',')
    ON_ERROR = 'SKIP_FILE_3';

SELECT * FROM ORDERS LIMIT 10;


--
-- 5. Skip a file when the percentage of error rows found in the file exceeds the specified percentage.
-- 

-- Set table back to no rows
TRUNCATE TABLE ORDERS;

-- Skips any file with a bad import
COPY INTO ORDERS
    FROM @aws_stage_errorex
    FILES = ('OrderDetails_error.csv', 'OrderDetails_error2.csv')
    FILE_FORMAT = (type = csv skip_header=1 field_delimiter=',')
    ON_ERROR = 'SKIP_FILE_3%';

SELECT * FROM ORDERS LIMIT 10;