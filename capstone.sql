--create databases
create or replace database RAW_DB;
create or replace database CLEANSED_DB;
 
use database RAW_DB;
 
 
--create schema
create schema RAW_DB.RAW_SCHEMA;
 
 
--create external storage integration
CREATE OR REPLACE STORAGE INTEGRATION capstone_csv
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::992382419355:role/caprole'
STORAGE_ALLOWED_LOCATIONS =('s3://capstonebuck/raw_csv/');
 
 
--Create external stage
CREATE OR REPLACE STAGE my_capstone
STORAGE_INTEGRATION = capstone_csv
URL = 's3://capstonebuck/raw_csv/';
 
desc integration capstone_csv;
 
 
--create tables
CREATE TABLE raw_transactions (
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP_NTZ,
    amount FLOAT,
    currency STRING,
    transaction_type STRING,
    channel STRING,
    merchant_name STRING,
    merchant_category STRING,
    location_country STRING,
    location_city STRING,
    is_flagged BOOLEAN
);
 
CREATE TABLE raw_customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    city STRING,
    country STRING,
    occupation STRING,
    income_bracket STRING,
    customer_since DATE
);
 
CREATE TABLE raw_accounts (
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT
);
 
CREATE TABLE raw_credit_data (
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
);
 
CREATE TABLE raw_watchlist (
    entity_id STRING,
    entity_name STRING,
    entity_type STRING,
    risk_category STRING,
    listed_date DATE,
    source STRING
);
 
--Create file formats
CREATE OR REPLACE FILE FORMAT csv_raw
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;
 
 
--create pipes
CREATE OR REPLACE PIPE raw_transactions_pipe
auto_ingest = true AS
COPY INTO raw_transactions
FROM @my_capstone
FILE_FORMAT = (FORMAT_NAME = 'RAW_DB.RAW_SCHEMA.csv_raw')
ON_ERROR = CONTINUE;
 
show pipes;
 
desc pipe raw_transactions_pipe;
 
 
CREATE OR REPLACE PIPE raw_customers_pipe
auto_ingest = true AS
COPY INTO raw_customers
FROM @my_capstone
FILE_FORMAT = (FORMAT_NAME = 'RAW_DB.RAW_SCHEMA.csv_raw')
ON_ERROR = CONTINUE;
 
 
CREATE OR REPLACE PIPE raw_accounts_pipe
auto_ingest = true AS
COPY INTO raw_accounts
FROM @my_capstone
FILE_FORMAT = (FORMAT_NAME = 'RAW_DB.RAW_SCHEMA.csv_raw')
ON_ERROR = CONTINUE;
 
 
CREATE OR REPLACE PIPE raw_credit_data_pipe
auto_ingest = true AS
COPY INTO raw_credit_data
FROM @my_capstone
FILE_FORMAT = (FORMAT_NAME = 'RAW_DB.RAW_SCHEMA.csv_raw')
ON_ERROR = CONTINUE;
 
 
CREATE OR REPLACE PIPE raw_watchlist_pipe
auto_ingest = true AS
COPY INTO raw_watchlist
FROM @my_capstone
FILE_FORMAT = (FORMAT_NAME = 'RAW_DB.RAW_SCHEMA.csv_raw')
ON_ERROR = CONTINUE;
 
 
alter pipe raw_transactions_pipe refresh;
alter pipe raw_customers_pipe refresh;
alter pipe raw_accounts_pipe refresh;
alter pipe raw_credit_data_pipe refresh;
alter pipe raw_watchlist_pipe refresh;
 
select * from raw_transactions;
select * from raw_customers;
select * from raw_accounts;
select * from raw_credit_data;
select * from raw_watchlist;




create or replace function transaction_func(amount number)

returns string

language sql 

asPC_DBT_DB.DBT_.TRANSACTION_MODELPC_DBT_DB.DBT_.TRANSACTION_MODEL

$$

select case when amount > 500 then 'High'

        when amount between 100 and 200 then 'Medium'

        else 'Low' end

$$

;


grant USAGE on FUNCTION transaction_func(NUMBER) to role PC_DBT_ROLE;

REVOKE APPLYBUDGET ON DATABASE RAW_DB FROM ROLE PC_DBT_ROLE;
grant all privileges on DATABASE RAW_DB to role PC_DBT_ROLE;
grant all privileges on schema RAW_DB.RAW_SCHEMA to role PC_DBT_ROLE;
grant select on all tables in schema RAW_DB.RAW_SCHEMA to role PC_DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE RAW_DB TO ROLE PC_DBT_ROLE;


---------------Query optimization----------------
 
CREATE MASKING POLICY GLOBALBANK.RAW_DATA.EMAIL_MASK AS
(EMAIL VARCHAR) RETURNS VARCHAR ->
CASE WHEN CURRENT_ROLE = 'ADMIN' THEN EMAIL
ELSE REGEXP_REPLACE(EMAIL, '.+\@', '*****@')
END;
ALTER TABLE GLOBALBANK.RAW_DATA.CUSTOMER_RAW MODIFY COLUMN email SET MASKING POLICY GLOBALBANK.RAW_DATA.EMAIL_MASK;
CREATE MASKING POLICY GLOBALBANK.RAW_DATA.Phone_MASK AS
(PHONE VARCHAR) RETURNS VARCHAR ->
CASE WHEN CURRENT_ROLE = 'ADMIN' THEN PHONE
ELSE SUBSTR(PHONE, 0, 5) || '***-****'
END;
ALTER TABLE GLOBALBANK.RAW_DATA.CUSTOMER_RAW MODIFY COLUMN phone_number SET MASKING POLICY GLOBALBANK.RAW_DATA.Phone_MASK;
CREATE OR REPLACE MASKING POLICY GLOBALBANK.RAW_DATA.customer_id_MASK AS
(Cust_id VARCHAR) RETURNS VARCHAR ->
CASE
WHEN CURRENT_ROLE() = 'ADMIN' THEN Cust_id
ELSE 'XXXXXX'
END;
ALTER TABLE GLOBALBANK.RAW_DATA.CUSTOMER_RAW MODIFY COLUMN phone_number SET MASKING POLICY GLOBALBANK.RAW_DATA.customer_id_MASK;