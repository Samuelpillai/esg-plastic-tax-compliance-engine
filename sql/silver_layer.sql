--- This SQL script creates the SALES_ENRICHED table in the ESG_SILVER schema by joining the raw sales, products, and dates tables. It applies heuristics to estimate plastic weight and classify ESG match status and tags based on product categories.
USE SCHEMA ESG_PL_DB.ESG_SILVER;

CREATE OR REPLACE TABLE SALES_ENRICHED AS
SELECT
    f.sales_id,
    p.product_id,
    p.product_name,
    p.category,
    p.brand,

    d.year,
    d.month,
    f.total_amount,

    -- Plastic weight heuristic
    CASE
        WHEN LOWER(p.category) LIKE '%home%' THEN 0.25
        WHEN LOWER(p.category) LIKE '%grocery%' THEN 0.15
        WHEN LOWER(p.category) LIKE '%electronics%' THEN 0.10
        WHEN LOWER(p.category) LIKE '%clothing%' THEN 0.05
        ELSE 0.08
    END AS plastic_weight_kg,

    -- ESG match status
    CASE
        WHEN LOWER(p.category) LIKE '%grocery%'
          OR LOWER(p.category) LIKE '%home%'
        THEN 'MATCHED'
        ELSE 'UNCLASSIFIED'
    END AS esg_match_status,

    -- ESG tag
    CASE
        WHEN LOWER(p.category) LIKE '%grocery%' THEN 'HIGH_PLASTIC_RISK'
        WHEN LOWER(p.category) LIKE '%electronics%' THEN 'LOW_PLASTIC_RISK'
        ELSE 'MEDIUM_PLASTIC_RISK'
    END AS esg_tag

FROM ESG_PL_DB.ESG_RAW.FACT_SALES_RAW f
JOIN ESG_PL_DB.ESG_RAW.DIM_PRODUCTS_RAW p
  ON f.product_sk = p.product_sk
JOIN ESG_PL_DB.ESG_RAW.DIM_DATES_RAW d
  ON TO_DATE(f.sales_date) = d.full_date;

--- Create a final enriched table with an additional column to flag unclassified records
CREATE OR REPLACE TABLE SALES_ENRICHED_FINAL AS
SELECT *,
    CASE
        WHEN esg_match_status = 'UNCLASSIFIED' THEN 1
        ELSE 0
    END AS is_unclassified
FROM SALES_ENRICHED;