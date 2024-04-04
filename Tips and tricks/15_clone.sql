USE DATABASE USERS_DB;
USE SCHEMA PESTYL;


-- LOAD DATA

CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
);

CREATE OR REPLACE STAGE aws_stage
    URL = 's3://bucketsnowflakes3';

LIST @aws_stage;

COPY INTO ORDERS
    FROM @aws_stage
    FILES = ('OrderDetails.csv')
    FILE_FORMAT = (type = 'csv', field_delimiter=',', skip_header=1);

-- PREVIEW
SELECT * FROM orders LIMIT 10;


-- 
-- CREATE CLONE
--
CREATE TABLE orders_clone
    CLONE orders;

SELECT * FROM orders_clone LIMIT 10;
SELECT count(*) FROM orders_clone;

-- Test that cloned table is different from original even though it's a metadata clone
UPDATE orders_clone
    SET ORDER_ID = NULL;
SELECT * FROM orders_clone LIMIT 10;

-- Test original table
SELECT * FROM orders LIMIT 10;


-- NOTE: If you clone a temp table, it must come from a temp table.


