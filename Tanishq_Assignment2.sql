CREATE DATABASE DB1

// Uploading sample data from snowflake to s3

//Create a sample snowflake table as below,

CREATE OR REPLACE TRANSIENT TABLE DB1.PUBLIC.CUSTOMER_TEST
AS
SELECT * FROM 
"SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER";

--create external storage integration
CREATE OR REPLACE STORAGE INTEGRATION mys3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::992382419355:role/mysnowrole'
STORAGE_ALLOWED_LOCATIONS =('s3://mysnowassgn/csv/');
 
desc INTEGRATION mys3_int;

--Create file format
create or replace file format new_format
type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true compression = gzip;


--Create external stage
CREATE OR REPLACE STAGE s3_new_stage
STORAGE_INTEGRATION = mys3_int
URL = 's3://mysnowassgn/csv/';


--Copy command
COPY INTO @DB1.PUBLIC.s3_new_stage/Customer_data/
from
DB1.PUBLIC.CUSTOMER_TEST;

//. QUERY DATA IN S3 FROM SNOWFLAKE.
SELECT $1 C_CUSTOMER_SK,
$2 C_CUSTOMER_ID ,
$3 C_CURRENT_CDEMO_SK ,
$4 C_CURRENT_HDEMO_SK ,
$5 C_CURRENT_ADDR_SK,
$6 C_FIRST_SHIPTO_DATE_SK ,
$7 C_FIRST_SALES_DATE_SK ,
$8 C_SALUTATION ,
$9 C_FIRST_NAME ,
$10 C_LAST_NAME,
$11 C_PREFERRED_CUST_FLAG ,
$12 C_BIRTH_DAY ,
$13 C_BIRTH_MONTH ,
$14 C_BIRTH_YEAR,
$16 C_LOGIN ,
$17 C_EMAIL_ADDRESS ,
$18 C_LAST_REVIEW_DATE
FROM @DB1.PUBLIC.S3_NEW_STAGE/Customer_data/. ---replace it with new stage 
(file_format => DB1.PUBLIC.NEW_FORMAT)

//Filter data directly from s3

SELECT $1 C_CUSTOMER_SK,
$2 C_CUSTOMER_ID ,
$3 C_CURRENT_CDEMO_SK ,
$4 C_CURRENT_HDEMO_SK ,
$5 C_CURRENT_ADDR_SK,
$6 C_FIRST_SHIPTO_DATE_SK ,
$7 C_FIRST_SALES_DATE_SK ,
$8 C_SALUTATION ,
$9 C_FIRST_NAME ,
$10 C_LAST_NAME,
$11 C_PREFERRED_CUST_FLAG ,
$12 C_BIRTH_DAY ,
$13 C_BIRTH_MONTH ,
$14 C_BIRTH_YEAR,
$16 C_LOGIN ,
$17 C_EMAIL_ADDRESS ,
$18 C_LAST_REVIEW_DATE
FROM @DB1.PUBLIC.S3_NEW_STAGE/Customer_data/
(file_format => DB1.PUBLIC.NEW_FORMAT)
WHERE C_CUSTOMER_SK ='64596949'


//Execute group by
SELECT $9 C_FIRST_NAME,$10 C_LAST_NAME,COUNT(*)
FROM @DB1.PUBLIC.S3_NEW_STAGE/Customer_data/
(file_format => DB1.PUBLIC.NEW_FORMAT)
GROUP BY $9,$10


//CREATE VIEW OVER S3 DATA
CREATE OR REPLACE VIEW CUSTOMER_DATA
AS
SELECT $1 C_CUSTOMER_SK,
$2 C_CUSTOMER_ID ,
$3 C_CURRENT_CDEMO_SK ,
$4 C_CURRENT_HDEMO_SK ,
$5 C_CURRENT_ADDR_SK,
$6 C_FIRST_SHIPTO_DATE_SK ,
$7 C_FIRST_SALES_DATE_SK ,
$8 C_SALUTATION ,
$9 C_FIRST_NAME ,
$10 C_LAST_NAME,
$11 C_PREFERRED_CUST_FLAG ,
$12 C_BIRTH_DAY ,
$13 C_BIRTH_MONTH ,
$14 C_BIRTH_YEAR,
$16 C_LOGIN ,
$17 C_EMAIL_ADDRESS ,
$18 C_LAST_REVIEW_DATE
FROM @DB1.PUBLIC.S3_NEW_STAGE/Customer_data/
(file_format => DB1.PUBLIC.NEW_FORMAT)


//Query data directly on view,
SELECT * FROM CUSTOMER_DATA;

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Question: Now we can directly query data from s3 through view. What is the disadvantage of using this approach ? Can you see partitions being scanned in the backend ?
 
--Answer: Disadvantages of this approach is: 
//1. More latency compared to querying data stored in Snowflake as it is time consuming because the data is fetched from S3 storage.
//2. S3 does not support indexing which can lead to inefficient queries especially with large datasets.
//3. Managing partitions manually in S3 can be complex and tend to give error.
 
//There is no partitions scanned in this process.

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

//Create a sample snowflake table as below
Create or replace transient table CUSTOMER_SNOWFLAKE_TABLE
AS
SELECT * FROM CUSTOMER_TEST limit 1000

//Join this with the view we created earlier,
SELECT B.* 
FROM CUSTOMER_SNOWFLAKE_TABLE B
LEFT OUTER JOIN 
CUSTOMER_DATA A
ON
A.C_CUSTOMER_SK = B.C_CUSTOMER_SK

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Question: Now we successfully joined data in s3 with snowflake table. It may look simple but this approach has lot of potential. Can you mention few below?
 
--Answer: The potentials are as follow: 
--1. Cost Saving - Storage
--2. Flexibility in storage capacity
--3. In-depth data explorations with external storage providers.

//----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Question: How many partitions got scanned from snowflake table?
 
--Answer: Partitions scanned are 356

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

//UNLOAD DATA BACK TO S3

COPY INTO @DB1.PUBLIC.S3_NEW_STAGE/Customer_joined_data/
from(
SELECT B.* 
FROM CUSTOMER_SNOWFLAKE_TABLE B
LEFT OUTER JOIN 
CUSTOMER_DATA A
ON
A.C_CUSTOMER_SK = B.C_CUSTOMER_SK)

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 6. ADVANTAGES AND DISADVANTAGES
 
--Advantages
--1. Low storage cost for large volumes of data
--2. Unlimited storage 
--3. Scalability
 
--Disadvantages
--1. Query performance is slow
--2. Complexity of data
--3. Additional charges for transferring data from s3 to snowflake



