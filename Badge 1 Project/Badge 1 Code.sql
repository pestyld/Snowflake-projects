
-- Badge 1: Data Warehousing Workshop


--
-- SETTINGS
--
USE ROLE SYSADMIN;

USE WAREHOUSE COMPUTE_WH;

USE DATABASE GARDEN_PLANTS;



-- CREATE TABLE
create or replace table GARDEN_PLANTS.VEGGIES.ROOT_DEPTH (
   ROOT_DEPTH_ID number(1), 
   ROOT_DEPTH_CODE text(1), 
   ROOT_DEPTH_NAME text(7), 
   UNIT_OF_MEASURE text(2),
   RANGE_MIN number(2),
   RANGE_MAX number(2)
   ); 


--
-- ADD DATA TO THE TABLE
-- 

-- INSERT ROWS INTO THE TABLE
INSERT INTO VEGGIES.ROOT_DEPTH (
	ROOT_DEPTH_ID ,
	ROOT_DEPTH_CODE ,
	ROOT_DEPTH_NAME ,
	UNIT_OF_MEASURE ,
	RANGE_MIN ,
	RANGE_MAX 
)
VALUES
(1,'S','Shallow','cm',30,45),
(2,'M','Medium','cm',45,60),
(3,'D','Deep','cm',60,90)
;

SELECT * FROM VEGGIES.ROOT_DEPTH;



--
-- CREATE VEGETABLE_DETAILS TABLE
--
CREATE TABLE vegetable_details (
    plant_name varchar(25),
    root_depth_code varchar(1)
)


--
-- CREATE FILE FORMAT
-- 
CREATE FILE FORMAT garden_plants.veggies.pipecolsep_oneheadrow
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
;

CREATE FILE FORMAT garden_plants.veggies.commasep_dblquot_oneheadrow
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;



--
-- REMOVING DUPLICATE ROW
--

-- Find duplicate  (spinach)
SELECT plant_name, count(*) as total
FROM vegetable_details
GROUP BY plant_name
HAVING total > 1
ORDER BY total desc;

-- Find row to delete
SELECT *
FROM vegetable_details
WHERE plant_name = 'Spinach' and
      root_depth_code = 'D';
    
-- Delete row
DELETE FROM vegetable_details
WHERE plant_name = 'Spinach' and root_depth_code = 'D';

SELECT plant_name, count(*) as total
FROM vegetable_details
GROUP BY plant_name
HAVING total > 1
ORDER BY total desc;





--
-- SETUP AUTO GRADE FOR BADGE 1
--

-- CREATE API INTEGRATION
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE API INTEGRATION dora_api_integration
    API_PROVIDER = aws_api_gateway
    API_AWS_ROLE_ARN = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole'
    ENABLED = true
    API_ALLOWED_PREFIXES = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

-- CREATE GRADER FUNCTION
create or replace external function util_db.public.grader(
      step varchar
    , passed boolean
    , actual integer
    , expected integer
    , description varchar)
returns variant
api_integration = dora_api_integration 
context_headers = (current_timestamp,current_account, current_statement) 
as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'
;

-- IS THE FUNCTION WORKING
use role accountadmin;
use database util_db; 
use schema public; 

select grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 

show user functions;



--
-- CREATE STAGE TO S3
-- 
CREATE STAGE like_a_window_into_an_s3_bucket 
	URL = 's3://https://uni-lab-files.s3.us-west-2.amazonaws.com/' 
	DIRECTORY = ( ENABLE = true );

-- LIST FILES IN STAGE
LIST @like_a_window_into_an_s3_bucket;

LIST @like_a_window_into_an_s3_bucket/this_;


--
-- LOADING DATA TO SNOWFLAKE FROM A STAGE
--

CREATE OR REPLACE TABLE garden_plants.veggies.vegetable_details_soil_type
(
    plant_name varchar(25),
    soil_type number(1,0)
)

COPY INTO garden_plants.veggies.vegetable_details_soil_type
    FROM @like_a_window_into_an_s3_bucket
    FILES = ('VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
    FILE_FORMAT = (format_name=GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW);


--
-- QUERY DATA IN A STAGE BEFORE LOAD
--

-- Query without file format
SELECT $1
FROM @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
LIMIT 5;

--Same file but with one of the file formats we created earlier  
SELECT $1,$2, $3
FROM @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

--Same file but with the other file format we created earlier
select $1, $2, $3
from @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW );


--
-- LOAD THE LU_SOIL_TYPE.tsv file
-- 

-- PREVIEW FILE
SELECT $1
FROM @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
LIMIT 5;

-- CREATE STAGE
CREATE OR REPLACE FILE FORMAT garden_plants.veggies.L8_CHALLENGE_FF
  TYPE = CSV
  FIELD_DELIMITER = '\t'
  SKIP_HEADER = 1;


USE ROLE SYSADMIN;
-- CREATE TABLE
CREATE OR REPLACE TABLE garden_plants.veggies.lu_soil_type (
    SOIL_TYPE_ID number,
    SOIL_TYPE varchar(15),
    SOIL_DESCRIPTION varchar(75)
);

COPY INTO garden_plants.veggies.lu_soil_type
    FROM @util_db.public.like_a_window_into_an_s3_bucket
    FILES = ('LU_SOIL_TYPE.tsv')
    FILE_FORMAT = (FORMAT_NAME = 'garden_plants.veggies.L8_CHALLENGE_FF')
    VALIDATION_MODE = RETURN_5_ROWS;

COPY INTO garden_plants.veggies.lu_soil_type
    FROM @util_db.public.like_a_window_into_an_s3_bucket
    FILES = ('LU_SOIL_TYPE.tsv')
    FILE_FORMAT = (FORMAT_NAME = 'garden_plants.veggies.L8_CHALLENGE_FF');

SELECT * FROM garden_plants.veggies.lu_soil_type;

--
-- LOAD THE LU_SOIL_TYPE.tsv file
-- 
USE ROLE SYSADMIN;
LIST @like_a_window_into_an_s3_bucket/veg_;

-- Preview
SELECT $1,$2,$3, $4, $5
FROM @like_a_window_into_an_s3_bucket/veg_plant_height.csv;

CREATE TABLE garden_plants.veggies.vegetable_details_plant_height (
    plant_name varchar(40),
    UOM char(1),
    Low_End_of_Range number,
    High_End_of_range number
);

SHOW FILE FORMATS in DATABASE garden_plants;

USE DATABASE garden_plants;
USE SCHEMA veggies;

COPY INTO garden_plants.veggies.vegetable_details_plant_height
    FROM @util_db.public.like_a_window_into_an_s3_bucket
    FILES = ('veg_plant_height.csv')
    FILE_FORMAT = (FORMAT_NAME = 'COMMASEP_DBLQUOT_ONEHEADROW')
    VALIDATION_MODE = RETURN_5_ROWS;

COPY INTO garden_plants.veggies.vegetable_details_plant_height
    FROM @util_db.public.like_a_window_into_an_s3_bucket
    FILES = ('veg_plant_height.csv')
    FILE_FORMAT = (FORMAT_NAME = 'COMMASEP_DBLQUOT_ONEHEADROW');

    
USE DATABASE util_db;
USE SCHEMA PUBLIC;
-- PLEASE EDIT THIS TO PUT YOUR EMAIL, FIRST, MIDDLE, & LAST NAMES 
--(remove the angle brackets and put single quotes around each value)
select util_db.public.greeting('styl4891@gmail.com', 'Peter', '', 'Styliadis');









--
--
-- NEXT DATABASE SECTION - LESSON 9
--
--
USE ROLE sysadmin;

// Create new database
CREATE DATABASE library_card_catalog
    COMMENT = 'DWW Lesson 9';

USE DATABASE LIBRARY_CARD_CATALOG;

// Create author table
CREATE TABLE author (
    AUTHOR_UID number,
    FIRST_NAME varchar(50),
    MIDDLE_NAME varchar(50),
    LAST_NAME varchar(50)
);

// Insert rows in table
INSERT INTO author (author_uid, first_name, middle_name, last_name)
    VALUES
    (1, 'Fiona', '','Macdonald'),
    (2, 'Gian','Paulo','Faleschini');
SELECT * FROM author;


// Create a sequence
CREATE OR REPLACE SEQUENCE library_card_catalog.public.seq_author_uid
    START = 1 
    INCREMENT = 1
    COMMENT = 'Use this to fill in author_uid';

--
-- Query the sequence
--
use role sysadmin;

-- See how the nextval function works
SELECT SEQ_AUTHOR_UID.nextval;

SHOW SEQUENCES;

CREATE OR REPLACE SEQUENCE LIBRARY_CARD_CATALOG.PUBLIC.SEQ_AUTHOR_UID
START 3 
INCREMENT 1 
COMMENT = 'Use this to fill in the AUTHOR_UID every time you add a row';

-- Insert remaining rows into table using the nextvalue in sequence as the ID.
INSERT INTO AUTHOR(AUTHOR_UID,FIRST_NAME,MIDDLE_NAME, LAST_NAME) 
Values
(SEQ_AUTHOR_UID.nextval, 'Laura', 'K','Egendorf')
,(SEQ_AUTHOR_UID.nextval, 'Jan', '','Grover')
,(SEQ_AUTHOR_UID.nextval, 'Jennifer', '','Clapp')
,(SEQ_AUTHOR_UID.nextval, 'Kathleen', '','Petelinsek');

SELECT * FROM AUTHOR;


--
-- CREATE SECOND COUNTER AND NEW TABLE 
--
CREATE OR REPLACE SEQUENCE library_card_catalog.public.seq_book_uid
    START 1
    INCREMENT 1
    COMMENT = 'Use this to fil the BOOK_UID everytime you add a row';

CREATE OR REPLACE TABLE library_card_catalog.public.BOOK
( 
  BOOK_UID NUMBER DEFAULT SEQ_BOOK_UID.nextval,
  TITLE VARCHAR(50),
  YEAR_PUBLISHED NUMBER(4,0)
);

// Insert records into the book table
// You don't have to list anything for the
// BOOK_UID field because the default setting
// will take care of it for you
INSERT INTO BOOK(TITLE,YEAR_PUBLISHED)
VALUES
 ('Food',2001)
,('Food',2006)
,('Food',2008)
,('Food',2016)
,('Food',2015);

SELECT * FROM library_card_catalog.public.book;


// Create the relationships table
// this is sometimes called a "Many-to-Many table"
CREATE OR REPLACE TABLE library_card_catalog.public.BOOK_TO_AUTHOR
(  BOOK_UID NUMBER
  ,AUTHOR_UID NUMBER
);

//Insert rows of the known relationships
INSERT INTO library_card_catalog.public.BOOK_TO_AUTHOR(BOOK_UID,AUTHOR_UID)
VALUES
 (1,1)  // This row links the 2001 book to Fiona Macdonald
,(1,2)  // This row links the 2001 book to Gian Paulo Faleschini
,(2,3)  // Links 2006 book to Laura K Egendorf
,(3,4)  // Links 2008 book to Jan Grover
,(4,5)  // Links 2016 book to Jennifer Clapp
,(5,6); // Links 2015 book to Kathleen Petelinsek

USE DATABASE library_card_catalog;
USE SCHEMA PUBLIC;

select * 
    from book_to_author ba 
    inner join author a on ba.author_uid = a.author_uid 
    inner join book b on b.book_uid=ba.book_uid; 



--
-- LOADING SEMI STRUCTURED DATA
--

USE ROLE sysadmin;
USE DATABASE library_card_catalog;
USE SCHEMA PUBLIC;


--
-- JSON LOAD
--

-- CREATE TABLE
CREATE TABLE author_ingest_json(
    RAW_AUTHOR variant
);

-- CREATE FILE FORMAT
CREATE OR REPLACE FILE FORMAT json_file_format
    TYPE = 'json'
    COMPRESSION = 'AUTO'
    STRIP_OUTER_ARRAY = TRUE; // If false, the array of objects will be loaded in one row. You want to strip the array and leave the objects (rows)

-- Find and view stage
SHOW STAGES in database UTIL_DB;
LIST @util_db.public.LIKE_A_WINDOW_INTO_AN_S3_BUCKET;

-- Copy into Snowflake table
COPY INTO author_ingest_json
    FROM @util_db.public.LIKE_A_WINDOW_INTO_AN_S3_BUCKET
    FILES = ('author_with_header.json')
    FILE_FORMAT = (format_name = 'json_file_format')
    --VALIDATION_MODE = RETURN_5_ROWS
    ;

-- Querying a variant data type
SELECT * FROM author_ingest_json;

-- Query the AUTHOR_UID key values
SELECT raw_author, raw_author:AUTHOR_UID
FROM author_ingest_json;

SELECT 
 raw_author:AUTHOR_UID
,raw_author:FIRST_NAME::STRING as FIRST_NAME
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
FROM AUTHOR_INGEST_JSON;


LIST @like_a_window_into_an_s3_bucket;


--
-- READING NESTED SEMI-STRUCTURE DATA
-- LOAD NEW FILE NESTED JSON DATA
--

-- CREATE TABLE
CREATE OR REPLACE TABLE library_card_catalog.public.nested_ingest_json(
    RAW_NESTED_BOOK variant
)

-- LOAD INTO TABLE
USE DATABASE library_card_catalog;
USE SCHEMA public;
COPY INTO library_card_catalog.public.nested_ingest_json
    FROM @util_db.public.LIKE_A_WINDOW_INTO_AN_S3_BUCKET
    FILES = ('json_book_author_nested.txt')
    FILE_FORMAT = (format_name = 'json_file_format')
    --VALIDATION_MODE = RETURN_5_ROWS
    ;

-- QUERY THE SEMI STRUCTURED DATA (5 total rows)
SELECT RAW_NESTED_BOOK
FROM NESTED_INGEST_JSON;

-- QUERY BASED ON FIRST LEVEL KEY
SELECT RAW_NESTED_BOOK:year_published
FROM NESTED_INGEST_JSON;

-- QUERY KEY THAT CONTAINS ARRY OF OBJECTS
SELECT RAW_NESTED_BOOK:authors
FROM NESTED_INGEST_JSON;

-- FLATTEN THE AUTHOR DATA WHICH CONTAINS ARRAY OF OBJECTS (now 6 rows)
SELECT value
FROM NESTED_INGEST_JSON,
    LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);

SELECT value:first_name
FROM NESTED_INGEST_JSON tbl ,
    LATERAL FLATTEN(input =>tbl.RAW_NESTED_BOOK:authors);

-- CAST RESULTS
SELECT value:first_name::varchar as First_NM,
       value:last_name::varchar as Last_NM
FROM NESTED_INGEST_JSON tbl,
    LATERAL FLATTEN(input => tbl.RAW_NESTED_BOOK:authors)


--
-- READING JSON TWITTER DATA
--

-- CREATE NEW DATABASE
CREATE DATABASE social_media_floodgates
  COMMENT = 'There\'s so much data from social media - flood warning';  

USE DATABASE social_media_floodgates;

-- CREATE TABLE IN NEW DATABASE
CREATE OR REPLACE TABLE SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST(
    RAW_STATUS variant
)
COMMENT = 'Bring in tweets, one row per tweet or status entity';

-- CREATE FILE FORMAT
CREATE FILE FORMAT SOCIAL_MEDIA_FLOODGATES.PUBLIC.JSON_FILE_FORMAT 
    TYPE = 'JSON' 
    COMPRESSION = 'AUTO' 
    ENABLE_OCTAL = FALSE 
    ALLOW_DUPLICATE = FALSE 
    STRIP_OUTER_ARRAY = TRUE 
    STRIP_NULL_VALUES = FALSE 
    IGNORE_UTF8_ERRORS = FALSE;

-- LIST FILES IN STAGE (lookin gfor nutrition_tweets.json)
LIST @util_db.public.like_a_window_into_an_s3_bucket/nutrition;


-- LOAD DATA
COPY INTO SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
    FROM @util_db.public.like_a_window_into_an_s3_bucket
    FILES = ('nutrition_tweets.json')
    FILE_FORMAT = (format_name = 'JSON_FILE_FORMAT')
    --VALIDATION_MODE = RETURN_5_ROWS
    ;

-- VIEW ROWS IN TABLE
SELECT RAW_STATUS 
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;

-- VIEW THE ENTITIES VALUES
SELECT RAW_STATUS:entities
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;

SELECT RAW_STATUS:entities:hashtags
FROM TWEET_INGEST;

-- VIEW ELEMENTS IN THE ARRAY OF HASHTAG
SELECT RAW_STATUS:entities:hashtags[0]
FROM TWEET_INGEST;

SELECT RAW_STATUS:entities:hashtags[0].text
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;

SELECT RAW_STATUS:entities:hashtags[0].text
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;
WHERE RAW_STATUS:entities:hashtags[0].text is not null;

-- PERFORM A SIMPLE CAST ON THE CREATED_AT KEY
SELECT RAW_STATUS:created_at::DATE as DATE
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;
ORDER BY DATE;

-- FLATTEN THE HASTAGS ENTITY VALUES

-- 9 original values
SELECT RAW_STATUS 
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;

-- 14 after flatten
SELECT value
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST ti,
    LATERAL FLATTEN(input=>ti.RAW_STATUS:entities.hashtags);

-- OR
SELECT value
FROM TWEET_INGEST,
    TABLE(FLATTEN(RAW_STATUS:entities:hashtags));

-- FLATTEN JUST THE TEXT KEY IN ENTITIES HASHTAGS
SELECT value:text
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST ti,
    LATERAL FLATTEN(input=>ti.RAW_STATUS:entities:hashtags);

-- MAKE VARCHAR AND NAME COLUMN
SELECT value:text::VARCHAR(30) as THE_HASHTAG
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST ti,
    LATERAL FLATTEN(input=>ti.RAW_STATUS:entities:hashtags);

-- ADD THE TWEET_ID and USER_ID to the table
SELECT *
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;

SELECT RAW_STATUS:user:id as USER_ID,
       RAW_STATUS:id as TWEET_ID,
       value:text:: VARCHAR as HASHTAG_TEXT
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST ti,
    LATERAL FLATTEN(input=> ti.RAW_STATUS:entities:hashtags);



-- CREATE VIEW OF NORMALIZED TWEET DATA
CREATE OR REPLACE VIEW SOCIAL_MEDIA_FLOODGATES.PUBLIC.HASHTAGS_NORMALIZED AS
SELECT RAW_STATUS:user:id as USER_ID,
       RAW_STATUS:id as TWEET_ID,
       VALUE:text::VARCHAR as HASHTAG_TEXT
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST as ti,
    LATERAL FLATTEN( INPUT => ti.RAW_STATUS:entities:hashtags );

SELECT * FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.HASHTAGS_NORMALIZED;













USE ROLE ACCOUNTADMIN;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT 'DWW19' as step
  ,( select count(*) 
    from SOCIAL_MEDIA_FLOODGATES.INFORMATION_SCHEMA.VIEWS 
    where table_name = 'HASHTAGS_NORMALIZED') as actual
  , 1 as expected
  ,'Check number of rows' as description
 ); 


 select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
   SELECT 'DWW18' as step
  ,( select row_count 
    from SOCIAL_MEDIA_FLOODGATES.INFORMATION_SCHEMA.TABLES 
    where table_name = 'TWEET_INGEST') as actual
  , 9 as expected
  ,'Check number of rows' as description  
 ); 


 -- Set your worksheet drop lists. DO NOT EDIT THE DORA CODE.
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT 'DWW16' as step
  ,( select row_count 
    from LIBRARY_CARD_CATALOG.INFORMATION_SCHEMA.TABLES 
    where table_name = 'AUTHOR_INGEST_JSON') as actual
  ,6 as expected
  ,'Check number of rows' as description
 ); 
