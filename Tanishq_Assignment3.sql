// Create warehouse
use warehouse NEW_WH;

// Create database
create database NEW_DB;

// External Stage S3 -
 
//  Create User in AWS with programmatic access and copy the credentials. 

// Create s3 bucket

//   Create Stage 


// Create external stage

CREATE STAGE NEW_STAGE
URL='s3://newsnowss/t1.json'
CREDENTIALS=(AWS_KEY_ID='AKIA6ODU2AGN4LWKMMSX' AWS_SECRET_KEY='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');


// CREATE table in Snowflake with VARIANT column

CREATE TABLE PERSON_NESTED (
    person VARIANT
);


// Create a Snowpipe with Auto Ingest Enabled

CREATE PIPE person_pipe AUTO_INGEST = TRUE AS
COPY INTO PERSON_NESTED
FROM (
    SELECT 
    OBJECT_CONSTRUCT(
        'ID', $1,
        'Name', $2,
        'Age', $3,
        'Location', $4,
        'Zip', IFF($5 = '' OR $5 IS NULL, '00000', $5),
        'Filename', METADATA$FILENAME,
        'FileRowNumber', METADATA$FILE_ROW_NUMBER,
        'IngestedTimestamp', TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    ) AS person
    FROM @NEW_STAGE
)
ON_ERROR = CONTINUE;

alter pipe person_pipe refresh;

show pipes;

// Subscribe the Snowflake SQS Queue in s3-

// Test Snowpipe by copying the sample JSON file and upload the file to s3 in path

select system$pipe_status('person_pipe');

show pipes;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    table_name => 'person_nested',
    start_time => DATEADD('hour', -24, CURRENT_TIMESTAMP())
));

select * from person_nested;

// Change Data Capture using Streams, Tasks and Merge

// Create Streams on PERSON_NESTED table to capture the change data on PERSON_NESTED table and use TASKS to Run SQL/Stored Procedure to Unnested the data from PERSON_NESTED and create PERSON_MASTER table //

create or replace stream person_stream on table person_nested;

show streams;

// Create PERSON_MASTER Table:

CREATE OR REPLACE TABLE PERSON_MASTER (
    id STRING,
    name STRING,
    age NUMBER,
    address STRING);
    
// Create task
CREATE TASK NEW_TASK
SCHEDULE = '1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('PERSON_NESTED_STREAM')
AS
MERGE INTO PERSON_MASTER pm
USING (
    SELECT data:id::STRING as id,
           data:name::STRING as name,
           data:age::NUMBER as age,
           data:address::STRING as address
    FROM PERSON_NESTED_STREAM
) sm
ON pm.id = sm.id
WHEN MATCHED THEN UPDATE SET pm.name = sm.name, pm.age = sm.age, pm.address = sm.address
WHEN NOT MATCHED THEN INSERT (id, name, age, address) VALUES (sm.id, sm.name, sm.age, sm.address);


// Test PIPELINE

// TRUNCATING THE DATA IN TABLES -

TRUNCATE TABLE PERSON_NESTED;
TRUNCATE TABLE PERSON_MASTER;
TRUNCATE TABLE PERSON_STREAM;

SELECT * FROM PERSON_NESTED;
SELECT * FROM PERSON_MASTER;

SELECT * FROM PERSON_STREAM;

SHOW PIPES;

SHOW TASKS;










