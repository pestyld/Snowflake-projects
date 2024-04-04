USE DATABASE USERS_DB;
USE SCHEMA PESTYL;

CREATE OR REPLACE TABLE TIMETRAVEL(
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



-- VALIDATE COPY INTO FIRST
COPY INTO timetravel
    FROM @time_travel_stage
    FILES = ('customers.csv')
    VALIDATION_MODE='RETURN_5_ROWS';


-- FULLY COPY DATA INTO TABLE
COPY INTO timetravel
    FROM @time_travel_stage
    FILES = ('customers.csv');

SELECT * FROM timetravel LIMIT 10;


--
-- UPDATE THE TIMETRAVEL TABLE BY MISTAKE (WHOOPS!)
--

UPDATE timetravel
    SET FIRST_NAME = 'Francesco';

SELECT * FROM timetravel LIMIT 10;


--
-- METHOD 1
-- 5 MINUTE BLOCK
--
SELECT * 
FROM timetravel at (OFFSET => -60*2)
LIMIT 10;



--
-- METHOD 2
-- TIMESTAMP
--

-- RECREATE TABLE
CREATE OR REPLACE TABLE TIMETRAVEL(
    ID INT,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    EMAIL STRING,
    GENEDER STRING,
    JOB STRING,
    PHONE STRING
);
COPY INTO timetravel
    FROM @time_travel_stage
    FILES = ('customers.csv');

SELECT * FROM timetravel LIMIT 10;

-- SET TIMEZONE
ALTER SESSION SET TIMEZONE='UTC';
SELECT CURRENT_TIMESTAMP;

-- 2024-01-04 17:52:08.344 +0000

-- UPDATE DATA WITH MISTAKE!
UPDATE timetravel
    SET Job = 'Data Engineer';
    
SELECT * FROM timetravel LIMIT 10;

--  USE TIMESTAMP TO SEE ORIGINAL DATA
SELECT *
FROM timetravel before (timestamp => '2024-01-04 17:52:08.344'::timestamp)
LIMIT 10;




--
-- METHOD 3
-- USE QUERY ID
--

-- RECREATE TABLE
CREATE OR REPLACE TABLE TIMETRAVEL(
    ID INT,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    EMAIL STRING,
    GENEDER STRING,
    JOB STRING,
    PHONE STRING
);
COPY INTO timetravel
    FROM @time_travel_stage
    FILES = ('customers.csv');

-- UPDATE DATA WITH MISTAKE!
UPDATE timetravel
    SET Job = 'Data Engineer';

-- QUERY ID: 01b172f2-0c05-7827-0000-0e65113c3e52

SELECT * 
FROM timetravel before (statement => '01b172f2-0c05-7827-0000-0e65113c3e52')
LIMIT 10;



--
-- RESTORE DATA USING TIME TRAVEL
--

-- Bad data, Job is all Data Engineer
SELECT * FROM timetravel LIMIT 10;

-- Original data
SELECT * 
FROM timetravel before (statement => '01b172f2-0c05-7827-0000-0e65113c3e52')
LIMIT 10;

-- 1. Create a new backup table with the original data using the old query ID
CREATE OR REPLACE TABLE timetravel_backup as
    SELECT * 
    FROM timetravel before (statement => '01b172f2-0c05-7827-0000-0e65113c3e52');

-- 2. Delete the original updated bad data
TRUNCATE timetravel;

-- 3. Insert the good data from the backup table into the original table
INSERT INTO timetravel
    SELECT * FROM timetravel_backup;

SELECT * FROM timetravel LIMIT 10;
    
