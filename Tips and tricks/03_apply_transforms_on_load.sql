-- Specify database and schema
USE DATABASE USERS_DB;
USE SCHEMA PESTYL;


-- LOAD CSV FILE FROM STAGE
CREATE OR REPLACE TABLE ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT
);


LIST @aws_stage_course;

-- APPLY TRANSFORMS DURING LOAD (SIMPLE)
COPY INTO ORDERS_EX
    FROM (SELECT s.$1, s.$2 FROM @aws_stage_course AS s)
    FILE_FORMAT = (type = csv field_delimiter = ',' skip_header = 1)
    FILES = ('OrderDetails.csv')

SELECT * FROM ORDERS_EX LIMIT 10;



-- APPLY TRANSFORMS DURING LOAD (CONDITIONAL)

CREATE OR REPLACE TABLE ORDERS_EX_COND (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    PROFITABLE_FLAG VARCHAR(30)
);


COPY INTO ORDERS_EX_COND
    FROM (SELECT 
            s.$1, 
            s.$2, 
            s.$3,
            CASE WHEN 
                CAST(s.$3 as int) <0 THEN 'Not profitable'
                ELSE 'PROFITABLE'
            END
          FROM @aws_stage_course AS s)
    FILE_FORMAT = (type = csv field_delimiter = ',' skip_header = 1)
    FILES = ('OrderDetails.csv');

SELECT * FROM ORDERS_EX_COND LIMIT 10;


-- APPLY TRANSFORMS DURING LOAD (SUBSTRING)

CREATE OR REPLACE TABLE ORDERS_EX_COND (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    SUBSTRING CHAR(7)
);

LIST @AWS_STAGE_COURSE;

COPY INTO ORDERS_EX_COND 
    FROM(SELECT
            s.$1,
            s.$2,
            s.$3,
            SUBSTR(s.$5,1,5)
         FROM @aws_stage_course as s)
    FILE_FORMAT = (type = csv field_delimiter = ',' skip_header = 1)
    FILES = ('OrderDetails.csv');
    
SELECT * FROM ORDERS_EX_COND LIMIT 10;


-- APPLY TRANSFORMS DURING LOAD (SELECTING SPECIFIC COLUMNS IN SNOWFLAKE TABLE)

CREATE OR REPLACE TABLE ORDERS_EX_COND (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    SUBSTRING CHAR(7)
);

COPY INTO ORDERS_EX_COND(ORDER_ID, PROFIT)
    FROM (
        SELECT
            s.$1,
            s.$3
        FROM @aws_stage_course as S
    )
    FILE_FORMAT = (type = csv field_delimiter = ',' skip_header = 1)
    FILES = ('OrderDetails.csv');

SELECT * FROM ORDERS_EX_COND LIMIT 10;



-- AUTO INCREMENT COLUMN IN A TABLE AS 'KEY'

CREATE OR REPLACE TABLE ORDERS_EX_COND (
    ORDER_ID number autoincrement start 1 increment 1,  -- SET AUTO INCREMENT
    AMOUNT INT,
    PROFIT INT,
    PROFITABLE_FLAG VARCHAR(30)
);

DESCRIBE TABLE ORDERS_EX_COND;


COPY INTO ORDERS_EX_COND(PROFIT, AMOUNT, PROFITABLE_FLAG)
    FROM (
        SELECT 
            s.$2,
            s.$3,
            CASE WHEN 
                s.$2 > 0 THEN 'Profitable'
                ELSE 'Not Profitable'
            END
        FROM @AWS_STAGE_COURSE AS S
    )
    FILE_FORMAT = (type = csv field_delimiter = ',' skip_header = 1)
    FILES = ('OrderDetails.csv');

SELECT * FROM ORDERS_EX_COND WHERE PROFIT < 0 LIMIT 100;