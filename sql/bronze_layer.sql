--- This script sets up the bronze layer in the ESG data lake, which includes creating file formats and a stage for raw data ingestion.
USE SCHEMA ESG_PL_DB.ESG_RAW;

CREATE OR REPLACE FILE FORMAT CSV_FMT
TYPE = CSV
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('', 'NULL', 'null');

CREATE OR REPLACE FILE FORMAT PARQUET_FMT
TYPE = PARQUET;

-- Create stage for raw data
CREATE OR REPLACE STAGE ESG_RAW_STAGE
FILE_FORMAT = CSV_FMT;

-- Create tables for raw data ingestion
CREATE OR REPLACE TABLE FACT_SALES_RAW (
    sales_sk NUMBER,
    sales_id STRING,
    customer_sk NUMBER,
    product_sk NUMBER,
    store_sk NUMBER,
    salesperson_sk NUMBER,
    campaign_sk NUMBER,
    sales_date TIMESTAMP,
    total_amount NUMBER(12,2)
);

COPY INTO FACT_SALES_RAW
FROM @ESG_RAW_STAGE/fact_sales_normalized.csv
FILE_FORMAT = (FORMAT_NAME = CSV_FMT)
ON_ERROR = 'CONTINUE';

--- Create dimension tables for raw data ingestion
CREATE OR REPLACE TABLE DIM_PRODUCTS_RAW (
    product_sk NUMBER,
    product_id STRING,
    product_name STRING,
    category STRING,
    brand STRING,
    origin_location STRING
);

COPY INTO DIM_PRODUCTS_RAW
FROM @ESG_RAW_STAGE/dim_products.csv
FILE_FORMAT = (FORMAT_NAME = CSV_FMT);

--- Create dimension tables for raw data ingestion
CREATE OR REPLACE TABLE DIM_DATES_RAW (
    full_date DATE,
    date_sk NUMBER,
    year NUMBER,
    month NUMBER,
    day NUMBER,
    weekday NUMBER,
    quarter NUMBER
);

COPY INTO DIM_DATES_RAW
FROM @ESG_RAW_STAGE/dim_dates.csv
FILE_FORMAT = (FORMAT_NAME = CSV_FMT);

--- Create dimension tables for raw data ingestion
CREATE OR REPLACE TABLE AMAZON_ECO_RAW (
    id STRING,
    title STRING,
    name STRING,
    category STRING,
    material STRING,
    brand STRING,
    price NUMBER(10,2),
    rating NUMBER(3,1),
    reviewsCount NUMBER,
    description STRING,
    url STRING,
    img_url STRING,
    inStock BOOLEAN,
    inStockText STRING
);

COPY INTO AMAZON_ECO_RAW
FROM @ESG_RAW_STAGE/amazon_eco-friendly_products.csv
FILE_FORMAT = (FORMAT_NAME = CSV_FMT);

--- Validation queries for raw data ingestion
SELECT COUNT(*) FROM FACT_SALES_RAW;
SELECT COUNT(*) FROM DIM_PRODUCTS_RAW;
SELECT COUNT(*) FROM DIM_DATES_RAW;
SELECT COUNT(*) FROM AMAZON_ECO_RAW;