USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

USE DATABASE restaurant_dwh;
USE SCHEMA silver;

CREATE OR REPLACE PROCEDURE load_data_to_silver()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    -- LIST @restaurant_dwh.silver.s3_ex_stage;

    -- load data into temp stageing table
    TRUNCATE TABLE restaurant_dwh.silver.stg_restaurant;
    COPY INTO restaurant_dwh.silver.stg_restaurant
    FROM '@restaurant_dwh.silver.s3_ex_stage'
    FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT') 
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'SKIP_FILE'
    PATTERN = '.*[.]parquet';


    -- merge query to track and update scd2 columns and insert new rows
    MERGE INTO restaurant_dwh.silver.restaurant_details rd
    USING (
        SELECT *,
            CURRENT_DATE() AS record_start_date,
            NULL AS record_end_date,
            TRUE AS is_current
        FROM restaurant_dwh.silver.stg_restaurant
        -- WHERE restaurant_id = 'R72'
    ) srd
    ON rd.zomato_url = srd.zomato_url
    AND rd.is_current = TRUE

    WHEN MATCHED AND (
        rd.name != srd.name OR
        rd.city != srd.city OR
        rd.area != srd.area OR
        rd.rating != srd.rating OR
        rd.rating_count != srd.rating_count OR
        rd.phone != srd.phone OR
        rd.phone1 != srd.phone1 OR
        rd.cusine != srd.cusine OR
        rd.cost_for_two != srd.cost_for_two OR
        rd.address != srd.address OR
        rd.latitude != srd.latitude OR
        rd.longitude != srd.longitude OR
        rd.timings != srd.timings OR
        rd.online_order != srd.online_order OR
        rd.table_reservation != srd.table_reservation OR
        rd.delivery_only != srd.delivery_only OR
        rd.famous_food != srd.famous_food
    ) THEN
    UPDATE SET rd.record_end_date = CURRENT_DATE(),
                rd.is_current = FALSE

    WHEN NOT MATCHED THEN
    INSERT (
        restaurant_id, zomato_url, name, city, area, rating, rating_count, phone, phone1, cusine,
        cost_for_two, address, latitude, longitude, timings, online_order,
        table_reservation, delivery_only, famous_food, record_start_date,
        record_end_date, is_current
    )
    VALUES (
        srd.restaurant_id, srd.zomato_url, srd.name, srd.city, srd.area, srd.rating, srd.rating_count,
        srd.phone, srd.phone1, srd.cusine, srd.cost_for_two, srd.address,
        srd.latitude, srd.longitude, srd.timings, srd.online_order,
        srd.table_reservation, srd.delivery_only, srd.famous_food,
        srd.record_start_date, srd.record_end_date, srd.is_current
    );


    -- insert query to insert updated rows new row
    INSERT INTO restaurant_dwh.silver.restaurant_details (
        restaurant_id, zomato_url, name, city, area, rating, rating_count, phone, phone1,
        cusine, cost_for_two, address, latitude, longitude, timings, online_order,
        table_reservation, delivery_only, famous_food, record_start_date,
        record_end_date, is_current
    )
    SELECT
        restaurant_id, zomato_url, name, city, area, rating, rating_count, phone, phone1,
        cusine, cost_for_two, address, latitude, longitude, timings, online_order,
        table_reservation, delivery_only, famous_food,
        CURRENT_DATE(), NULL, TRUE
    FROM restaurant_dwh.silver.stg_restaurant srd
    WHERE EXISTS (
        SELECT 1 FROM restaurant_details rd
        WHERE rd.zomato_url = srd.zomato_url
        AND rd.is_current = FALSE
        AND rd.record_end_date = CURRENT_DATE()
    );
END
$$
