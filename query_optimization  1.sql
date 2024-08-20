---------- 3. Optimizing Query Performance with Clustering Keys --------------------------

-- A. Clustering by customer_id
ALTER TABLE raw_transactions
CLUSTER BY (customer_id);

-- B. Clustering by amount
ALTER TABLE raw_transactions
CLUSTER BY (amount);

-- C. Clustering by transaction_type and channel
ALTER TABLE raw_transactions
CLUSTER BY (transaction_type, channel);


----------- 2. Implementing Materialized Views for Frequently Accessed Data -----------
CREATE OR REPLACE MATERIALIZED VIEW mv_customer_summary AS
SELECT 
    customer_id, 
    COUNT(transaction_id) AS total_transactions, 
    SUM(amount) AS total_spent, 
    AVG(amount) AS avg_spent
FROM raw_transactions
GROUP BY customer_id;

----------------------------------------------------------------------------------

---------- 3. Strategy for efficiently managing single-cluster warehouses -----------------
--- A. Setting auto-suspend after 5 minutes of inactivity
ALTER WAREHOUSE CAP_WH
SET AUTO_SUSPEND = 300;

--- B. Enabling auto-resume to automatically start the warehouse when a query is run
ALTER WAREHOUSE CAP_WH
SET AUTO_RESUME = TRUE;

--- C. Resize the warehouse based on your workload requirements
ALTER WAREHOUSE CAP_WH
SET WAREHOUSE_SIZE = 'MEDIUM';

--- D. Use resource monitors to track and control warehouse usage.
CREATE OR REPLACE RESOURCE MONITOR monitor_warehouse_usage
WITH CREDIT_QUOTA = 500  -- Set a credit limit
TRIGGERS ON 80 PERCENT DO NOTIFY  -- Notify when 80% of credits are used
TRIGGERS ON 100 PERCENT DO SUSPEND;  -- Suspend warehouses when 100% of credits are used





-------------4. Security and Compliance ----------------
---------------1. DATA MASKING Policy ----------------

CREATE MASKING POLICY RAW_DB.GLOBALSCHEMA.EMAIL_MASK AS
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
 
ALTER TABLE GLOBALBANK.RAW_DATA.RAW_CUSTOMER MODIFY COLUMN phone_number SET MASKING POLICY GLOBALBANK.RAW_DATA.customer_id_MASK;



-------------------- B. Creating secure views for data sharing across departments
-- Create a secure view for the Finance department with limited columns
CREATE OR REPLACE SECURE VIEW finance_customer_view AS
SELECT 
    customer_id,
    first_name,
    last_name,
    income_bracket,
    current_balance
FROM raw_customers
WHERE CURRENT_ROLE() = 'FINANCE_ROLE';

-- Create a secure view for the HR department with different limited columns
CREATE OR REPLACE SECURE VIEW hr_customer_view AS
SELECT 
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    address,
    city,
    country
FROM raw_customers
WHERE CURRENT_ROLE() = 'HR_ROLE';