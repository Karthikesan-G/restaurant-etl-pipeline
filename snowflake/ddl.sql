USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- create db
CREATE DATABASE IF NOT EXISTS restaurant_dwh;

-- create schema
CREATE OR REPLACE SCHEMA restaurant_dwh.silver;
CREATE OR REPLACE SCHEMA restaurant_dwh.gold;
