/*
===============================================================================
Gold Layer: Business-Ready Views
===============================================================================
Purpose:
    Defines analytical views in the Gold layer by exposing business-ready
    dimensions and fact tables built on top of the Silver layer.

What This Layer Provides:
    - Conformed dimensions (Customer, Product)
    - Fact table for Sales transactions
    - Clean, readable column names for reporting
    - Star-schema friendly structures for BI tools

Design Notes:
    - Implemented using views for real-time access to Silver data
    - No transformations beyond business alignment and joins
    - Filters applied to expose only active/current records where required

===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customer
-- =============================================================================

IF OBJECT_ID('gold.dim_customer','V') IS NOT NULL
	DROP VIEW gold.dim_customer;
GO

CREATE VIEW gold.dim_customer AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key generated for analytics
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS firstname,
	la.cntry AS country,-- Country sourced from ERP location reference
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status,
	CASE				-- Prefer CRM gender; fallback to ERP if missing
		WHEN cst_gndr != 'n/a' THEN cst_gndr
		ELSE COALESCE (gen, 'n/a')
	END AS gender,
	ca.bdate AS birthday,
	ci.cst_create_date AS create_date	
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid  -- Join ERP demographics
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.cid  -- Join ERP location
GO

-- =============================================================================
-- Create Dimension: gold.dim_product
-- =============================================================================

IF OBJECT_ID('gold.dim_product','V') IS NOT NULL
	DROP VIEW gold.dim_product;
GO

CREATE VIEW gold.dim_product AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt) AS product_key,-- Surrogate key ordered by product lifecycle start
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	prd_line AS product_line,
	prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filer out all the historical data
GO

-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================

IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT	
	sd.sls_ord_num AS order_number,
	pr.product_number,
	cu.customer_id,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_customer AS cu
ON sd.sls_cust_id = cu.customer_id
LEFT JOIN gold.dim_product AS pr
ON sd.sls_prd_key = pr.product_number
GO

IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
    sd.sls_ord_num AS order_number,

    -- Product and customer attributes resolved via dimensions
    pr.product_number,
    cu.customer_id,

    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS ship_date,
    sd.sls_due_dt   AS due_date,

    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_customer AS cu
    ON sd.sls_cust_id = cu.customer_id   -- Customer dimension join
LEFT JOIN gold.dim_product AS pr
    ON sd.sls_prd_key = pr.product_number; -- Product dimension join
GO
