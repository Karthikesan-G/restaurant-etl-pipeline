# Restaurant ETL Pipeline

## Overview
This project implements an **ETL (Extract, Transform, Load) pipeline** for processing restaurant data (e.g., restaurant_id, address, zomato_url, cusines, famous_food) from raw CSV files stored in **AWS S3**. The pipeline extracts data, transforms it using **PySpark**, and loads it into a **Snowflake** data warehouse with a **silver layer** (historical data using SCD2) and a **gold layer** (star schema for analytics). A **data catalog** is created for the raw data using **AWS Glue**, enabling querying with **Amazon Athena** for ad-hoc analysis. The workflow is orchestrated to run **daily** using **Apache Airflow**.

## Features
- **Extract**: Reads raw CSV files from `s3://restaurant-details-dataset/rawData/`.
- **Data Catalog**: Uses AWS Glue to crawl and catalog raw data in S3, enabling querying with Amazon Athena for raw data analysis.
- **Transform**: Cleans and processes data using PySpark (e.g., removes nulls, standardizes formats, aggregates metrics).
- **Load**: 
  - Writes cleaned data to `s3://restaurant-details-dataset/cleanedData/`.
  - Loads cleaned data into Snowflake’s silver layer using an external stage, maintaining historical data with SCD2.
  - Creates a gold layer with a star schema (fact and dimension tables) for optimized analytics.
- **Automation**: Schedules daily ETL jobs via Airflow DAGs.
- **Scalability**: Leverages PySpark for distributed processing, Snowflake for querying, and Glue/Athena for scalable data cataloging.

## Tech Stack
- **AWS S3**: Stores raw CSV files and cleaned data.
- **AWS Glue**: Crawls raw data to create a data catalog for querying.
- **Amazon Athena**: Queries raw data in S3 for ad-hoc analysis.
- **PySpark**: Distributed data processing for cleaning and transformations.
- **Snowflake**: Cloud data warehouse for silver (SCD2) and gold (star schema) layers.
- **Apache Airflow**: Orchestrates daily pipeline execution.
- **Docker**: Containerizes Airflow for portable and consistent deployment.
- **Python**: Core language for scripting (version 3.8+).

## Project Structure
```
restaurant-etl-pipeline/
├── .gitignore                        
├── requirements.txt                 
│
├── venv/                             
│   └── ...                            
│
├── dags/                              
│   └── restaurant_etl_dag.py
│
├── dataset/
│   ├── raw/                         
│   │   └── india_all_restaurants_details.rar
│   └── cleaned/                      
│       └── part-00000-...parquet
│
├── diagrams/                         
│   ├── DataFlow_architecture_diagram.drawio
│   └── Gold_layer_ER_diagram.drawio
│
├── images/                           
│   ├── airflow_dag.png
│   ├── Gold_layer_ER_diagram.drawio.png
│   └── snowflake_dwh.png
│
├── pyspark_scripts/                  
│   ├── clean_restaurant_details.ipynb
│   └── clean_restaurant_details.py
│
├── snowflake/                        
│   ├── ddl.sql
│   ├── task.sql
│   ├── silver/
│   │   ├── create_stage.sql
│   │   ├── create_table.sql
│   │   ├── load_data.sql
│   │   └── load_data_procedure.sql
│   └── gold/
│       ├── load_table.sql
│       └── load_table_procedure.sql

```

## Prerequisites
- **AWS Account**: 
  - S3 bucket (`s3://restaurant-details-dataset/`) with `rawData/` and `cleanedData/` folders.
  - IAM roles for S3, Glue, and Athena access.
- **Snowflake Account**: Configured instance with external stage, silver, and gold schemas.
- **Apache Airflow**: Local or cloud-based setup (version 2.5+).
- **Docker**: Installed for running Airflow containers (Docker Desktop or Docker Engine).
- **PySpark**: Version 3.3+.
- **Python**: Version 3.8 or higher.
- **AWS Glue**: Configured crawler for raw data cataloging.
- **Amazon Athena**: Set up with a workgroup for querying.


## Usage
- The pipeline runs **daily** as defined in `dags/restaurant_etl_dag.py`.
- **Workflow**:
  1. **Extract**: Reads CSV files (e.g., `india_all_restaurants_details.csv`) from `s3://restaurant-details-dataset/rawData/`.
  2. **Catalog**: AWS Glue crawls raw data, creating a catalog in `restaurant_raw_db` for Athena querying.
  3. **Transform**: Uses PySpark to clean data (e.g., remove nulls, standardize columns, aggregate sales).
  4. **Load to S3**: Writes cleaned CSVs to `s3://restaurant-details-dataset/cleanedData/`.
  5. **Load to Snowflake Silver Layer**: Loads data into `silver.restaurant_details` using an external stage, applying SCD2 for historical tracking.
  6. **Create Gold Layer**: Transforms silver data into a star schema (e.g., `gold.dim_cuisine`, `gold.dim_service_features`) for analytics.
- **Querying Raw Data**: Use Athena to query raw data in `restaurant_raw_db` (e.g., `SELECT * FROM restaurant_raw_db.menu_data`).
- Monitor execution via Airflow UI (`http://localhost:8080`).
- Query Snowflake gold layer for insights (e.g., sales trends, popular menu items).

## Snowflake Schema
- **Silver Layer** (SCD2 for historical data):
  - Table: `restaurant_dwh.silver.restaurant_details`  
    - Columns: `restaurant_id`, `zomato_url`, `name`, `city`, `area`, `rating`, `rating_count`, `phone`, `phone1`, `cusine`, `cost_for_two`, `address`, `latitude`, `longitude`, `timings`, `online_order`, `table_reservation`, `delivery_only`, `famous_food`, `record_start_date`, `record_end_date`, `is_current`

    - SCD2 tracks changes with start/end dates and flag.
- **Gold Layer** (Star Schema):
  - Fact Table: `gold.fct_restaurant`
    - Columns: `restaurant_id`, `location_id`, `rating_id`, `food_id`, `cuisine_id`, `service_id`
  - Dimension Tables:
    - `restaurant_dwh.gold.dim_restaurant`: `restaurant_id`, `name`, `address`, `phone`, `phone1`, `timings`, `cost_for_two`, `zomato_url`
    - `restaurant_dwh.gold.dim_cuisine`: `cuisine_id`, `cusine_name`
    - `restaurant_dwh.gold.dim_famous_food`: `food_id`, `famous_food`
    - `restaurant_dwh.gold.dim_location`: `location_id`, `city`, `area`, `latitude`, `longitude`
    - `restaurant_dwh.gold.dim_service_features`: `service_id`, `online_order`, `table_reservation`, `delivery_only`
    - `restaurant_dwh.gold.dim_rating`: `rating_id`, `rating`, `rating_count`


## Future Improvements
- Implement incremental loads for raw and silver layer data.
- Add error handling for Glue crawler failures or S3 connectivity issues.
- Optimize PySpark jobs with dynamic partitioning for large datasets.
- Integrate monitoring with AWS CloudWatch for Glue and Airflow, and Snowflake query logs.

## Contact
For issues or contributions, open a GitHub issue or contact via GitHub.
