--- This SQL script creates the UK_PLASTIC_TAX_RATE table in the ESG_GOLD schema and inserts the current tax rate for plastic in the UK, effective from April 1, 2022. This table will be used to calculate the plastic tax liability for companies based on their estimated plastic weight.
USE SCHEMA ESG_PL_DB.ESG_GOLD;

CREATE OR REPLACE TABLE UK_PLASTIC_TAX_RATE (
    effective_date DATE,
    tax_rate_gbp_per_tonne NUMBER(10,2),
    is_active BOOLEAN
);

INSERT INTO UK_PLASTIC_TAX_RATE VALUES
('2022-04-01', 223.69, TRUE);

--- Create a view to calculate projected tax liability based on the enriched sales data and the active tax rate
CREATE OR REPLACE VIEW FACT_COMPLIANCE AS
SELECT
    s.sales_id,
    s.product_id,
    s.product_name,
    s.category,
    s.brand,
    s.year,
    s.month,
    s.total_amount,
    s.plastic_weight_kg,
    s.is_unclassified,

    -- Tax calculation
    (s.plastic_weight_kg / 1000) * t.tax_rate_gbp_per_tonne AS projected_tax_liability_gbp,

    -- BI date column
    DATE_FROM_PARTS(s.year, s.month, 1) AS year_month_date,

    s.esg_match_status,
    s.esg_tag

FROM ESG_PL_DB.ESG_SILVER.SALES_ENRICHED_FINAL s
JOIN ESG_PL_DB.ESG_GOLD.UK_PLASTIC_TAX_RATE t
  ON t.is_active = TRUE;

--- Create a summary table to analyze tax liability by category
CREATE OR REPLACE TABLE CATEGORY_RISK AS
SELECT
    category,
    COUNT(*) AS sales_count,
    SUM(projected_tax_liability_gbp) AS total_tax,
    SUM(is_unclassified) AS unclassified_sales
FROM FACT_COMPLIANCE
GROUP BY category;

--- Create a monthly trend table to analyze tax liability over time
CREATE OR REPLACE TABLE MONTHLY_ESG_TREND AS
SELECT
    year,
    month,
    SUM(plastic_weight_kg) AS total_plastic_kg,
    SUM(total_amount) AS total_sales,
    SUM(projected_tax_liability_gbp) AS total_tax_gbp,
    DATE_FROM_PARTS(year, month, 1) AS year_month_date
FROM FACT_COMPLIANCE
GROUP BY year, month;

--- Sample queries to analyze the compliance data
SELECT COUNT(*) FROM FACT_COMPLIANCE;

SELECT
  category,
  SUM(projected_tax_liability_gbp) AS total_tax
FROM FACT_COMPLIANCE
GROUP BY category
ORDER BY total_tax DESC;

SELECT
  MIN(projected_tax_liability_gbp),
  MAX(projected_tax_liability_gbp)
FROM FACT_COMPLIANCE;