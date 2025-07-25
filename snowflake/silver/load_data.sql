USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;


CALL restaurant_dwh.silver.load_data_to_silver();