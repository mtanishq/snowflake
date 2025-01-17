// Create Warehouses
CREATE WAREHOUSE IMPORT_WH01
  WITH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 300
  INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE TRANSFORM_WH01
  WITH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 300
  INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE REPORTING_WH01
  WITH
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 900
  MAX_CLUSTER_COUNT = 5
  MIN_CLUSTER_COUNT = 1
  SCALING_POLICY = 'ECONOMY'
  INITIALLY_SUSPENDED = TRUE;

// Create Databases
CREATE DATABASE STAGING01;
CREATE DATABASE PROD01;

// Create Schemas
CREATE SCHEMA STAGING01.RAW01;
CREATE SCHEMA STAGING01.CLEAN01;
CREATE SCHEMA PROD01.REPORTING01;

// Set Data Retention
ALTER SCHEMA STAGING01.RAW01 SET DATA_RETENTION_TIME_IN_DAYS = 3;
ALTER SCHEMA STAGING01.CLEAN01 SET DATA_RETENTION_TIME_IN_DAYS = 3;
ALTER SCHEMA PROD01.REPORTING01 SET DATA_RETENTION_TIME_IN_DAYS = 90;

// Create Roles
CREATE ROLE IMPORT_ROLE01;
CREATE ROLE TRANSFORM_ROLE01;
CREATE ROLE REPORTING_ROLE01;

// Grant privileges to IMPORT_ROLE
GRANT USAGE ON WAREHOUSE IMPORT_WH01 TO ROLE IMPORT_ROLE01;
GRANT USAGE ON DATABASE STAGING01 TO ROLE IMPORT_ROLE01;
GRANT USAGE ON SCHEMA STAGING01.RAW01 TO ROLE IMPORT_ROLE01;
GRANT USAGE ON SCHEMA STAGING01.CLEAN01 TO ROLE IMPORT_ROLE01;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA STAGING01.RAW01 TO ROLE IMPORT_ROLE01;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA STAGING01.CLEAN01 TO ROLE IMPORT_ROLE01;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA STAGING01.RAW01 TO ROLE IMPORT_ROLE01;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA STAGING01.CLEAN01 TO ROLE IMPORT_ROLE01;

// Grant privileges to TRANSFORM_ROLE
GRANT USAGE ON WAREHOUSE TRANSFORM_WH01 TO ROLE TRANSFORM_ROLE01;
GRANT USAGE ON DATABASE STAGING01 TO ROLE TRANSFORM_ROLE01;
GRANT USAGE ON SCHEMA STAGING01.CLEAN01 TO ROLE TRANSFORM_ROLE01;
GRANT USAGE ON DATABASE PROD01 TO ROLE TRANSFORM_ROLE01;
GRANT USAGE ON SCHEMA PROD01.REPORTING01 TO ROLE TRANSFORM_ROLE01;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA STAGING01.CLEAN01 TO ROLE TRANSFORM_ROLE01;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA PROD01.REPORTING01 TO ROLE TRANSFORM_ROLE01;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA STAGING01.CLEAN01 TO ROLE TRANSFORM_ROLE01;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA PROD01.REPORTING01 TO ROLE TRANSFORM_ROLE01;

// Grant privileges to REPORTING_ROLE
GRANT USAGE ON WAREHOUSE REPORTING_WH01 TO ROLE REPORTING_ROLE01;
GRANT USAGE ON DATABASE PROD01 TO ROLE REPORTING_ROLE01;
GRANT USAGE ON SCHEMA PROD01.REPORTING01 TO ROLE REPORTING_ROLE01;
GRANT SELECT ON ALL TABLES IN SCHEMA PROD01.REPORTING01 TO ROLE REPORTING_ROLE01;

// Create Users
CREATE USER UserImport PASSWORD = 'ABCD1'
  DEFAULT_ROLE = IMPORT_ROLE01
  DEFAULT_WAREHOUSE = IMPORT_WH01
  MUST_CHANGE_PASSWORD = TRUE;

CREATE USER UserTransform PASSWORD = 'ABCD2'
  DEFAULT_ROLE = TRANSFORM_ROLE01
  DEFAULT_WAREHOUSE = TRANSFORM_WH01
  MUST_CHANGE_PASSWORD = TRUE;

CREATE USER UserReporting PASSWORD = 'ABCD3'
  DEFAULT_ROLE = REPORTING_ROLE01
  DEFAULT_WAREHOUSE = REPORTING_WH01
  MUST_CHANGE_PASSWORD = TRUE;

// Assign Roles to Users
GRANT ROLE IMPORT_ROLE01 TO USER UserImport;
GRANT ROLE TRANSFORM_ROLE01 TO USER UserTransform;
GRANT ROLE REPORTING_ROLE01 TO USER UserReporting;

// Create Resource Monitors
CREATE RESOURCE MONITOR IMPORT_MONITOR WITH CREDIT_QUOTA = 100;
CREATE RESOURCE MONITOR TRANSFORM_MONITOR WITH CREDIT_QUOTA = 100;
CREATE RESOURCE MONITOR REPORTING_MONITOR WITH CREDIT_QUOTA = 100;

// Assign Resource Monitors to Warehouses
ALTER WAREHOUSE IMPORT_WH01 SET RESOURCE_MONITOR = IMPORT_MONITOR;
ALTER WAREHOUSE TRANSFORM_WH01 SET RESOURCE_MONITOR = TRANSFORM_MONITOR;
ALTER WAREHOUSE REPORTING_WH01 SET RESOURCE_MONITOR = REPORTING_MONITOR;