--
-- CREATE FIRST STAGE
--


-- Specify database and schema
USE DATABASE USERS_DB;
USE SCHEMA PESTYL;


-- CREATE EXTERNAL STAGE
CREATE OR REPLACE STAGE aws_stage_course
    url = 's3://bucketsnowflakes3'
    --credentials= (aws_key_id='abc' aws_secret_key='1234')
    ;

-- DESCRIBE STAGE
DESCRIBE STAGE aws_stage_course;

-- ALTER STAGE
ALTER STAGE aws_stage_course;

-- LIST FILES IN A STAGE
LIST @aws_stage_course;



-- LOAD CSV FILE FROM STAGE
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QuANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
);

COPY INTO ORDERS
    FROM @aws_stage_course
    FILE_FORMAT = (type = csv field_delimiter=',' skip_header = 1)
    PATTERN = '.*Order.*';

SELECT * FROM ORDERS LIMIT 10;