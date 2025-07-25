USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

USE DATABASE restaurant_dwh;
USE SCHEMA gold;

CREATE OR REPLACE PROCEDURE load_data_to_gold()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    -- dimension tables
    -- create dim table for cuisne
    CREATE OR REPLACE TABLE restaurant_dwh.gold.dim_cuisine as
    SELECT 
    concat('C', ROW_NUMBER() OVER (ORDER BY TRIM(cusine_name))) AS cuisine_id,
    cusine_name
    FROM (
    SELECT DISTINCT TRIM(c.value) AS cusine_name
    FROM restaurant_dwh.silver.restaurant_details,
        LATERAL FLATTEN (input => cusine) c
    WHERE is_current = TRUE
    );

    -- create dim table for famous_food
    CREATE OR REPLACE TABLE restaurant_dwh.gold.dim_famous_food as
    SELECT 
    concat('F', ROW_NUMBER() OVER (ORDER BY TRIM(famous_food))) AS food_id,
    famous_food
    FROM (
    SELECT DISTINCT TRIM(ff.value) AS famous_food
    FROM restaurant_dwh.silver.restaurant_details,
        LATERAL FLATTEN (input => famous_food) ff
    WHERE is_current = TRUE
    );

    -- create dim table for dim_location
    CREATE OR REPLACE TABLE restaurant_dwh.gold.dim_location AS
    SELECT 
        CONCAT('L', ROW_NUMBER() OVER (ORDER BY city DESC)) AS location_id,
        city, 
        area, 
        latitude, 
        longitude
    FROM restaurant_dwh.silver.restaurant_details
    WHERE is_current = TRUE
    GROUP BY city, area, latitude, longitude;

    -- create dim table for dim_service_features
    CREATE OR REPLACE TABLE restaurant_dwh.gold.dim_service_features AS
    SELECT 
        CONCAT('S', ROW_NUMBER() OVER (ORDER BY online_order DESC)) AS service_id,
        online_order, 
        table_reservation, 
        delivery_only
    FROM restaurant_dwh.silver.restaurant_details
    WHERE is_current = TRUE
    GROUP BY online_order, table_reservation, delivery_only;

    -- create dim table for dim_rating
    CREATE OR REPLACE TABLE restaurant_dwh.gold.dim_rating AS
    SELECT 
        CONCAT('RA', ROW_NUMBER() OVER (ORDER BY rating)) AS rating_id,
        rating, 
        rating_count
    FROM restaurant_dwh.silver.restaurant_details
    WHERE is_current = TRUE
    GROUP BY rating, rating_count;

    -- create dim table for dim_restaurant
    CREATE OR REPLACE TABLE restaurant_dwh.gold.dim_restaurant AS
    SELECT 
        restaurant_id,
        name, 
        address,
        phone,
        phone1,
        timings,
        cost_for_two,
        zomato_url
    FROM restaurant_dwh.silver.restaurant_details
    WHERE is_current = TRUE;


    -- fact table
    -- create fct table for restaurant with primary and foreign keys
    CREATE OR REPLACE TABLE restaurant_dwh.gold.fct_restaurant AS
    WITH exploded_table AS (
        SELECT 
            restaurant_id,
            city,
            area,
            latitude,
            longitude,
            rating,
            rating_count,
            TRIM(ff.value) AS famous_food,
            table_reservation,
            delivery_only,
            online_order,
            TRIM(c.value) AS cusine
        FROM restaurant_dwh.silver.restaurant_details,
            LATERAL FLATTEN (INPUT => cusine) c,
            LATERAL FLATTEN (INPUT => famous_food) ff
        WHERE is_current = TRUE
    ),
    joined_table AS (
        SELECT 
            et.restaurant_id,
            dt.location_id,
            dr.rating_id,
            dff.food_id,
            dc.cuisine_id,
            dsf.service_id
        FROM exploded_table et
        LEFT JOIN restaurant_dwh.gold.dim_location dt 
            ON dt.city = et.city 
            AND dt.area = et.area 
            AND dt.latitude = et.latitude 
            AND dt.longitude = et.longitude
        LEFT JOIN restaurant_dwh.gold.dim_rating dr 
            ON dr.rating = et.rating 
            AND dr.rating_count = et.rating_count
        LEFT JOIN restaurant_dwh.gold.dim_famous_food dff 
            ON dff.famous_food = et.famous_food
        LEFT JOIN restaurant_dwh.gold.dim_cuisine dc 
            ON dc.cusine_name = et.cusine
        LEFT JOIN restaurant_dwh.gold.dim_service_features dsf 
            ON dsf.table_reservation = et.table_reservation 
            AND dsf.delivery_only = et.delivery_only 
            AND dsf.online_order = et.online_order
    )
    SELECT * 
    FROM joined_table;

END
$$