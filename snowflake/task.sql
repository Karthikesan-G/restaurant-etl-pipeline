USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

USE DATABASE restaurant_dwh;

CREATE OR REPLACE TASK restaurant_load_trigger
    SCHEDULE = 'USING CRON 0 0 1 1 * UTC'
AS 
BEGIN
    CALL restaurant_dwh.silver.load_data_to_silver();
    CALL restaurant_dwh.gold.load_data_to_gold();
END;