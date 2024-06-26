USE USERS_DB;


--
-- LOAD HISTORY AT A DATABASE LEVEL
--

SELECT *
FROM INFORMATION_SCHEMA.LOAD_HISTORY;


--
-- LOAD HISTORY AT A GLOBAL LEVEL
--
SELECT *
FROM SNOWFLAKE.INFORMATION_SCHEMA.LOAD_HISTORY
WHERE SCHEMA_NAME

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY;