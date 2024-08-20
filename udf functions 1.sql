--------------2. Applying cleansing and Transformation rule-----------------
---------------1. creating UDFs--------------------------
create or replace function transaction_func(amount number)
returns string
language sql 
as
$$
select case when amount > 500 then 'High'
        when amount between 100 and 200 then 'Medium'
        else 'Low' end
$$
;

grant USAGE on FUNCTION transaction_func(NUMBER) to role PC_DBT_ROLE;


--select * from raw_transaction ;------------------
SELECT *, 
       RAW_DB.GLOBALSCHEMA.transaction_func(amount) AS risk_level 
FROM RAW_DB.GLOBALSCHEMA.raw_transactions;


REVOKE APPLYBUDGET ON DATABASE raw_db FROM ROLE PC_DBT_ROLE;
 grant all privileges on DATABASE raw_db to role PC_DBT_ROLE;
 grant all privileges on schema GLOBALschema to role PC_DBT_ROLE;
 grant select on all tables in schema GLOBALschema to role PC_DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE raw_db TO ROLE PC_DBT_ROLE;


----------2. function for cleaning DOB(null values and >100 and >0)-----------------
CREATE OR REPLACE FUNCTION cleanse_dob(customer_since DATE, date_of_birth DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
  CASE 
    -- If date_of_birth is NULL, replace it with customer_since - 18 years
    WHEN date_of_birth IS NULL THEN DATEADD(YEAR, -18, customer_since)
    -- If age is greater than 100 years or less than 0, replace with customer_since - 18 years
    WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE) > 100 
         OR DATEDIFF(YEAR, date_of_birth, CURRENT_DATE) < 0 THEN DATEADD(YEAR, -18, customer_since)
    -- Otherwise, keep the original date_of_birth
    ELSE date_of_birth
  END
$$;

grant USAGE on FUNCTION cleanse_dob(DATE, DATE) to role PC_DBT_ROLE;

SELECT 
    customer_id,
    first_name,
    last_name,
    cleanse_dob(customer_since, date_of_birth) AS cleansed_dob
FROM 
    raw_customers;


-------------3.UDF for cleanse first_name and last_name--------------
CREATE OR REPLACE FUNCTION cleanse_first_name(first_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  REGEXP_REPLACE(first_name, '[^a-zA-Z0-9]', '')
$$;

CREATE OR REPLACE FUNCTION cleanse_last_name(last_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  REPLACE(last_name, ' ', '')
$$;

SELECT
  customer_id,
  cleanse_first_name(first_name) AS first_name_cleaned,
  cleanse_last_name(last_name) AS last_name_cleaned
FROM
  raw_customers;

grant USAGE on FUNCTION cleanse_first_name(STRING) to role PC_DBT_ROLE;
grant USAGE on FUNCTION cleanse_last_name(STRING) to role PC_DBT_ROLE;

------------------------------------------------------------------
SELECT 
    customer_id,
    first_name,
    last_name,
    pc_dbt_db.public.cleanse_dob(customer_since, date_of_birth) AS cleansed_dob
FROM 
    pc_dbt_db.public.raw_customers