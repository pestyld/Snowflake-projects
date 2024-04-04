USE DATABASE USERS_DB;
USE SCHEMA PESTYL;


--
-- CREATE DEMO DATA
--

CREATE OR REPLACE TABLE customers(
    ID INT,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    EMAIL STRING,
    GENEDER STRING,
    JOB STRING,
    PHONE STRING
);

-- CREATE FILE FORMAT FOR COPY INTO
CREATE OR REPLACE FILE FORMAT csv_file
    TYPE = csv
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;


-- CREATE STAGE TO DATA LOCATION
CREATE OR REPLACE STAGE time_travel_stage
    URL = 's3://data-snowflake-fundamentals/time-travel/'
    FILE_FORMAT = csv_file;

LIST @time_travel_stage;

-- FULLY COPY DATA INTO TABLE
COPY INTO customers
    FROM @time_travel_stage
    FILES = ('customers.csv');



--
-- UNDROPPING TABLES
--
DROP TABLE customers;

-- Table doesn't exist
SELECT * FROM customers LIMIT 10;

UNDROP TABLE customers;

-- Table exists
SELECT * FROM customers LIMIT 10;



--
-- UNDROPPING SCHEMAS
--
DROP SCHEMA --<schema name>

UNDROP SCHEMA --<schema name>


--
-- UNDROPPING DATABASES
--
DROP DATABASE --<database name>>

UNDROP DATABASE --<database name>>




--
-- UNDROPPING TABLES AND TIME TRAVEL
--
SELECT * FROM customers LIMIT 10;

-- Make table update mistakes
UPDATE customers
    SET LAST_NAME = 'Tyson';

UPDATE customers
    SET JOB = 'Data Analyst';

SELECT * FROM customers LIMIT 10;

-- Undropping a table with a name that already exists.
-- For example, use time travel to fix updates but replace the bad table with the wrong query id time travel
CREATE OR REPLACE TABLE customers as
    SELECT *
    FROM customers before(statement =>'01b1773d-0c05-7ce8-0000-0e651149c15a');

-- Didn't use the correct query ID and the table is still incorrect
SELECT * FROM customers LIMIT 10;

-- Try to undrop the table that was updated. The issue is there is a table that is already named customers since the CREATE OR REPLACE dropped customers, then created customers
UNDROP TABLE customers;

-- Rename current customers table
ALTER TABLE customers 
    RENAME to customers_wrong;

-- Now undrop customers
UNDROP TABLE customers;

-- The incorrect table with LAST_NAME Tyson and JOB Data Analyst is back
SELECT * FROM customers LIMIT 10;

-- Use correct query ID before both updates
CREATE OR REPLACE TABLE customers as
    SELECT *
    FROM customers before(statement =>'01b1773d-0c05-7ce8-0000-0e651149c156');

-- Back to the table prior to the two updates
SELECT * FROM customers LIMIT 10;
