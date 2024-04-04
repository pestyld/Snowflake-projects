
--
-- LOAD AND VALIDATE DATA WITH ERRORS
-- steps to handle errors that may occur after data loading in Snowflake, discover effective error processing techniques to ensure 
-- data integrity and reliability and identify, and resolve potential issues vital for maintaining high-quality data and accurate analysis.
--


-- Specify database and schema
USE DATABASE USERS_DB;
USE SCHEMA PESTYL;

-- Create empy table
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
);


-- Create stage to data
-- Stage creates valid CSV files
CREATE OR REPLACE STAGE aws_stage_validate
    url = 's3://snowflakebucket-copyoption/size/';

-- List files in stage
LIST @aws_stage_validate;


--
-- 1. Use RETURN_ERRORS
-- Will return errors in the results if they exist
-- No errors in this data
-- No results and no data is loaded here
--
COPY INTO ORDERS
    FROM @aws_stage_validate
    PATTERN='.*Order.*'
    FILE_FORMAT = (type = csv 
                   field_delimiter=','
                   skip_header=1)
    VALIDATION_MODE=RETURN_ERRORS;       
    
SELECT * FROM ORDERS LIMIT 10;


-- Use RETURN_5_ROWS
-- Will show 5 rows from the data but not load it
COPY INTO ORDERS
    FROM @aws_stage_validate
    PATTERN='.*Order.*'
    FILE_FORMAT = (type = csv 
                   field_delimiter=','
                   skip_header=1)
    VALIDATION_MODE=RETURN_5_ROWS;       
    
SELECT * FROM ORDERS LIMIT 10;



--- USE INVALID CSV FILES
-- Create stage with invalid rows in the CSV file 
CREATE OR REPLACE STAGE aws_stage_validate
    url = 's3://snowflakebucket-copyoption/returnfailed/';

LIST @aws_stage_validate;

LIST @aws_stage_validate PATTERN='.*Order.*';


--
-- 2. VALIDATION_MODE=RETURN_ERRORS
-- View all errors in CSV files
-- Errows will show in the results
--
COPY INTO ORDERS
    FROM @aws_stage_validate
    PATTERN='.*Order.*'
    FILE_FORMAT = (type=csv
                field_delimiter=','
                skip_header=1)
    VALIDATION_MODE=return_errors;


-- View n rows
COPY INTO ORDERS
    FROM @aws_stage_validate
    PATTERN='.*error.*'
    FILE_FORMAT = (type=csv field_delimiter=',' skip_header=1)
    VALIDATION_MODE=return_5_rows


-- RECREATE TABLE TO START FROM THE BEGINNING
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
);

list @aws_stage_validate;


-- Return ALL errors on load
COPY INTO ORDERS
    FROM @aws_stage_validate
    PATTERN='.*Order.*'
    FILE_FORMAT=(type = csv field_delimiter=',' skip_header=1)
    VALIDATION_MODE=return_errors;


-- Store the error results from the last query
CREATE OR REPLACE TABLE rejected as
    SELECT * FROM table(result_scan(last_query_id()));

SELECT * FROM rejected;



--
-- 3. ON_ERROR = CONTINUE
-- Continue processing over errors
-- Results show some files were LOADED and some files were PARTIALLY_LOADED on the status column. 
-- Results show how many were not loaded and why.
--
COPY INTO ORDERS
    FROM @aws_stage_validate
    PATTERN='.*Order.*'
    FILE_FORMAT=(type = csv field_delimiter=',' skip_header=1)
    ON_ERROR=continue;

-- Use validation mode after the ON_ERROR=continue
SELECT * FROM table(validate(orders, job_id => '_last'));

-- View errors from saved table
SELECT rejected_record FROM rejected;



--
-- 4. Fixing rejected records
-- 
CREATE OR REPLACE TABLE rejected_clean as
SELECT
    split_part(rejected_record,',',1) as ORDER_ID,
    split_part(rejected_record,',',2) as AMOUNT,
    split_part(rejected_record,',',3) as PROFIT,
    split_part(rejected_record,',',4) as QUANTITY,
    split_part(rejected_record,',',5) as CATEGORY,
    split_part(rejected_record,',',6) as SUBCATEGORY
FROM rejected;

SELECT * FROM rejected_clean;