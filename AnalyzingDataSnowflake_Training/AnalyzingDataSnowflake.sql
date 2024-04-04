USE DATABASE users_db;

USE SCHEMA pestyl;


-- 1. Create  empty tables
CREATE OR REPLACE TABLE movies (
    ID INTEGER NOT NULL,
    TITLE VARCHAR(1000),
    BUDGET INTEGER,
    RELEASE_DATE DATE,
    REVENUE INTEGER,
    TAGLINE VARCHAR(1500),
    RATING FLOAT,
    DIRECTOR_ID INTEGER NOT NULL
    )
    COMMENT = 'Movie information';

    
CREATE OR REPLACE TABLE directors (
        NAME VARCHAR(250),
        ID INTEGER NOT NULL,
        GENDER INTEGER
    )
    COMMENT = 'Directors info';


-- 2. Create file format
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  TRIM_SPACE = TRUE
  FILE_EXTENSION = 'csv'
  COMMENT = 'Format of CSV file';


-- 3. Use SNOWSQL to load from client to snowflake stage
-- Login to snowsql: snowsql -a <account> -u <user>
-- PUT file:///C:\Users\xx\path/file.csv @~;
-- View internal stage files: ls @~;


-- 4. Load data from internal stage

-- List files in stage
LIST @~;

-- a. Load into movies Snowflake from internal
COPY INTO movies
    FROM @~/movies.csv.gz
    FILE_FORMAT = (FORMAT_NAME = csv_format)
    VALIDATION_MODE = RETURN_5_ROWS;

COPY INTO movies
    FROM @~/movies.csv.gz
    FILE_FORMAT = (FORMAT_NAME = csv_format);

-- b. Load into directors Snowflake from internal
COPY INTO directors
    FROM @~/directors.csv.gz
    FILE_FORMAT = (FORMAT_NAME = csv_format)
    VALIDATION_MODE = RETURN_5_ROWS;

COPY INTO directors
    FROM @~/directors.csv.gz
    FILE_FORMAT = (FORMAT_NAME = csv_format);


    
-- 5. Preview tables
SELECT * FROM movies LIMIT 10;
SELECT * FROM directors LIMIT 10;


-- 6. Filter dates
SELECT *
FROM MOVIES
WHERE release_date BETWEEN '2012-01-01' and '2012-12-31';


--6. Create a DateBucket filter

-- agg selector
SELECT 
    MAX(release_date) as Agg_Release_Date,
    COUNT(*) as TotalReleased
FROM
    MOVIES
GROUP BY 
    :datebucket(release_date)
ORDER BY
    Agg_Release_Date ASC;

-- daterange
SELECT 
    *
FROM
    MOVIES
WHERE
    release_date = :daterange
ORDER BY
    release_date;


-- 7. Examine query profile
SELECT
    m.title, 
    d.name
FROM 
    movies as m
    inner join directors as d on d.id = m.director_id
WHERE
    d.name like 'Steven%'
ORDER BY 
    d.name;


-- 8. Time travel
CREATE OR REPLACE TABLE movies_2017 (
        ID INTEGER NOT NULL,
        TITLE VARCHAR(150),
        BUDGET INTEGER,
        RELEASE_DATE DATE,
        REVENUE INTEGER,
        TAGLINE STRING,
        RATING FLOAT,
        DIRECTOR_ID INTEGER NOT NULL
    )
    DATA_RETENTION_TIME_IN_DAYS=0;

CREATE OR REPLACE TABLE movies_2016 (
        ID INTEGER NOT NULL,
        TITLE VARCHAR(150),
        BUDGET INTEGER,
        RELEASE_DATE DATE,
        REVENUE INTEGER,
        TAGLINE STRING,
        RATING FLOAT,
        DIRECTOR_ID INTEGER NOT NULL
    )
    DATA_RETENTION_TIME_IN_DAYS=1;

-- View information about the tables
SHOW TABLES LIKE 'movies_%';

-- Insert into movies_2016
INSERT INTO MOVIES_2016
    SELECT * 
    FROM MOVIES 
    WHERE release_date between '2016-01-01' and '2016-12-31';

-- Alter time zone
ALTER SESSION SET TIMEZONE = 'UTC';

-- Get current date and time
SELECT GETDATE();
--2024-01-26 12:32:11.556 +0000

-- Delete movies under 100,000
DELETE FROM movies_2016
    WHERE BUDGET < 100000;
-- Query ID: 01b1ed71-0a05-9122-0000-0e6511d5d3fa

-- Count rows of table with deleted rows
SELECT COUNT(*) FROM movies_2016;
-- 86 rows

-- time travel with query ID
SELECT COUNT(*) 
FROM movies_2016 BEFORE (statement => '01b1ed71-0a05-9122-0000-0e6511d5d3fa');
-- 104

-- time travel with time stamp
SELECT COUNT(*) 
FROM movies_2016 AT(timestamp => '2024-01-26 12:32:11.556 +0000'::timestamp);
-- 104

-- time travel with offset
SELECT COUNT(*) 
FROM movies_2016 BEFORE(offset => -60*7);

-- Drop table
DROP TABLES movies_2016



-- 9. Variable substitution
SET (MINVALUE, MAXVALUE)=(40, 70);
SELECT $MINVALUE;

SET FIRST_NAME='Adam';

SELECT *
FROM directors 
WHERE NAME LIKE $FIRST_NAME || '%';

SHOW VARIABLES;


-- 10. Clustering and search 

-- create tables for comparisons
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.PARTSUPP
LIMIT 10;

CREATE OR REPLACE TABLE partsupp_clustered AS
    SELECT *
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.PARTSUPP;

CREATE OR REPLACE TABLE partsupp_search_optimized AS
    SELECT *
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.PARTSUPP;

-- Add clustering and searc optimization to specified tables
ALTER TABLE partsupp_clustered CLUSTER BY (PS_SUPPKEY);

ALTER TABLE partsupp_search_optimized ADD search optimization;

-- View information about the tables and notice the new values
SHOW TABLES LIKE 'PARTSUPP%';

-- Compare an equality query using the clustering key

-- about 2.2 seconds, 1.2 GB scanned
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.PARTSUPP
WHERE PS_SUPPKEY = 834724;

-- 52 ms, 30MB scanned
SELECT *
FROM partsupp_clustered
WHERE PS_SUPPKEY = 834724;

-- 40 ms, 700MB scanned
SELECT *
FROM partsupp_search_optimized
WHERE PS_SUPPKEY = 834724;


-- Compare a ranged query using the clustering key

-- about 9.2 seconds, 1.3 GB scanned
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.PARTSUPP
WHERE PS_SUPPKEY = 834724 and PS_SUPPKEY < 934724;

-- 1.2 ms, 300MB scanned
SELECT *
FROM partsupp_clustered
WHERE PS_SUPPKEY = 834724 and PS_SUPPKEY < 934724;

-- 8 seconds, 1.2GB scanned
SELECT *
FROM partsupp_search_optimized
WHERE PS_SUPPKEY = 834724 and PS_SUPPKEY < 934724;



-- 11. Views

-- View query profile. You can see which tables were used.
CREATE OR REPLACE VIEW movies_and_directors AS
    SELECT title, tagline, release_date, name
    FROM movies INNER JOIN directors ON movies.director_id = directors.id;

SELECT * FROM movies_and_directors LIMIT 10;


-- Create secure view. View query profile after limit query. Notice you can't see the tables in the profile.
CREATE OR REPLACE SECURE VIEW movies_and_directors AS
    SELECT title, tagline, release_date, name
    FROM movies INNER JOIN directors ON movies.director_id = directors.id;

SELECT * FROM movies_and_directors LIMIT 10;


-- 12. Snowpipe local files

-- Log into snowpipe
-- Login to snowsql: snowsql -a <account> -u <user>
-- PUT file:///C:\Users\xx\path/file.csv @~;
-- View internal stage files: ls @~;

-- View all files in internal stage. Find uk_customers.csv.gz
LIST @~;

CREATE OR REPLACE TABLE customers_uk (
    ID INTEGER NOT NULL,
    NAME VARCHAR(100),
    SURNAME VARCHAR(100),
    GENDER VARCHAR(10),
    AGE INTEGER,
    REGION VARCHAR(100),
    JOB_CLASSIFICATION VARCHAR(100),
    BALANCE FLOAT
);

-- Load file into snowflake table. Notice errors (header issues). COntinue load anyway.
COPY INTO customers_uk
    FROM @~
    FILES = ('uk_customers.csv.gz')
    FILE_FORMAT = (type = 'csv' field_delimiter = ',')
    ON_ERROR = CONTINUE;

-- view all errors
SELECT * FROM TABLE(validate(customers_uk, job_id => '_last'));

-- View bad table
SELECT * FROM customers_uk LIMIT 10;

-- Fix loading into Snowflake. Do a test with validation_mode
COPY INTO customers_uk
    FROM @~
    FILES = ('uk_customers.csv.gz')
    FILE_FORMAT = (type = 'csv' 
                   field_delimiter = ','
                   skip_header = 1
                   field_optionally_enclosed_by = '''')
    VALIDATION_MODE = RETURN_10_ROWS;

-- Fix loading into Snowflake.
COPY INTO customers_uk
    FROM @~
    FILES = ('uk_customers.csv.gz')
    FILE_FORMAT = (type = 'csv' 
                   field_delimiter = ','
                   skip_header = 1
                   field_optionally_enclosed_by = '''');


-- Delete table
TRUNCATE TABLE customers_uk;



-- 13. Create internal stage
LIST @~;

CREATE OR REPLACE STAGE customers_data_stage
    FILE_FORMAT = (TYPE = 'csv'
                   FIELD_DELIMITER = ','
                   SKIP_HEADER = 1
                   FIELD_OPTIONALLY_ENCLOSED_BY = '''');

SHOW STAGES;

-- use PUT in SNOWSQL to load file into stage

-- Confirm file loaded to stage
LIST @customers_data_stage;

-- Copy staged file into Snowflake table
COPY INTO customers_uk
    FROM @customers_data_stage;

-- Preview table
SELECT * FROM customers_uk LIMIT 10;



-- 14. Sample rows in table
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.ITEM
SAMPLE ROW(100 rows);

-- 1 % of the data (any percentage you want)
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.ITEM
SAMPLE ROW(1);

-- Repeatable sampling
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.ITEM
SAMPLE ROW(.05) repeatable(111);


-- Bernoulli sampling
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.ITEM
SAMPLE BERNOULLI(.01) ;

-- SYSTEM sampling
-- Includes each block of rows with a probability of p/100. Similar to flipping a weighted coin for each block of rows. This method does not support fixed-size sampling.
SELECT *
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.ITEM
SAMPLE SYSTEM(.01) ;