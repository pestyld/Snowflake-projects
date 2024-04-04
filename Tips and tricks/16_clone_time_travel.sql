USE DATABASE USERS_DB;
USE SCHEMA PESTYL;

-- Load data

CREATE OR REPLACE TABLE time_travel (
    id int,
    first_name string,
    last_name string,
    email string,
    gender string,
    job string,
    phone string
);

CREATE OR REPLACE FILE FORMAT csv_file
    TYPE = csv
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

CREATE OR REPLACE STAGE time_travel_stage
    URL = 's3://data-snowflake-fundamentals/time-travel/'
    FILE_FORMAT = csv_file;

LIST @time_travel_stage;

COPY INTO time_travel
    FROM @time_travel_stage;

SELECT * FROM time_travel LIMIT 10;

-- Make an update mistake
UPDATE time_travel
    SET FIRST_NAME = 'Peter';
SELECT * FROM time_travel LIMIT 10;


-- Use time travel
SELECT * FROM time_travel at (STATEMENT => '01b1bab1-0c05-85f5-0000-0e651193e506') LIMIT;



--
-- CLONE USING TIME TRAVEL
--
CREATE OR REPLACE TABLE time_travel_clone
    CLONE time_travel at (STATEMENT => '01b1bab1-0c05-85f5-0000-0e651193e506');

SELECT * FROM time_travel_clone LIMIT 10;
    