USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

USE DATABASE restaurant_dwh;
USE SCHEMA silver;

-- creating storage integration and config it in aws
CREATE OR REPLACE STORAGE INTEGRATION s3_to_snowflake
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::137386360306:role/snowflake-s3-access'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://restaurant-details-dataset/cleanedData/')

DESC STORAGE INTEGRATION s3_to_snowflake;

-- create file format
CREATE OR REPLACE FILE FORMAT parquet_format
    TYPE = PARQUET 
    COMPRESSION = 'AUTO'

-- create stage
CREATE OR REPLACE STAGE s3_ex_stage
    STORAGE_INTEGRATION = s3_to_snowflake
    URL = 's3://restaurant-details-dataset/cleanedData/'
    FILE_FORMAT = parquet_format