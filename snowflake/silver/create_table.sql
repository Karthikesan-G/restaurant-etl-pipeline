USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- create table in silver
CREATE TABLE IF NOT EXISTS restaurant_dwh.silver.restaurant_details
(
    restaurant_id VARCHAR NOT NULL, 
    zomato_url VARCHAR NOT NULL, 
    name VARCHAR,
    city VARCHAR,  
    area VARCHAR, 
    rating FLOAT, 
    rating_count INT, 
    phone VARCHAR, 
    phone1 VARCHAR, 
    cusine VARIANT, 
    cost_for_two INT, 
    address VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT, 
    timings VARIANT, 
    online_order BOOLEAN, 
    table_reservation BOOLEAN, 
    delivery_only BOOLEAN, 
    famous_food VARIANT,
    record_start_date DATE,
    record_end_date DATE,
    is_current BOOLEAN
);

-- create stage table
CREATE OR REPLACE TABLE restaurant_dwh.silver.stg_restaurant AS 
SELECT restaurant_id, 
    zomato_url, 
    name, 
    city, 
    area, 
    rating, 
    rating_count, 
    phone, 
    phone1, 
    cusine, 
    cost_for_two, 
    address, 
    latitude, 
    longitude, 
    timings, 
    online_order, 
    table_reservation, 
    delivery_only, 
    famous_food 
FROM restaurant_dwh.silver.restaurant_details