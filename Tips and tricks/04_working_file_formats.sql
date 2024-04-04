-- Specify database and schema
USE DATABASE USERS_DB;
USE SCHEMA PESTYL;

-- VIEW INFORMATION ABOUT THE STAGE
DESCRIBE STAGE aws_stage_course;


-- CREATE TABLE AND COPY INTO USING STATICALLY TYPED FILE FORMAT
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
);

COPY INTO ORDERS
    FROM @AWS_STAGE_COURSE
    FILES = ('OrderDetails.csv')
    FILE_FORMAT = (type = csv field_delimiter = ',' skip_header = 1);

SELECT * FROM ORDERS LIMIT 10;

    
-- CREATE FILE FORMAT OBJECT INDEPENDENT FROM THE STAGE
CREATE OR REPLACE FILE FORMAT my_file_format
    TYPE = CSV
    COMMENT = 'First file format';

DESCRIBE FILE FORMAT my_file_format;



-- USE FILE FORMAT WITH COPY INTO
COPY INTO ORDERS
    FROM @aws_stage_course
    FILES = ('OrderDetails.csv')
    FILE_FORMAT = my_file_format;

SELECT * FROM ORDERS LIMIT 10;

DESCRIBE FILE FORMAT my_file_format;


-- ALTER A FILE FORMAT
ALTER FILE FORMAT my_file_format
    SET SKIP_HEADER=1;

DESCRIBE FILE FORMAT my_file_format;

