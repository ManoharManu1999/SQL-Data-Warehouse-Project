

/*
===============================================================================
Stored Procedure: silver.load_silver
===============================================================================

Purpose:
    Loads and transforms data from the Bronze layer into the Silver layer
    of the Data Warehouse.

    The Silver layer represents cleansed, standardized, and conformed data
    that is ready for analytical use and downstream modeling.

What This Procedure Does:
    - Performs full-refresh loads (TRUNCATE + INSERT) for all Silver tables
    - Cleans and standardizes text fields (TRIM, UPPER, formatting)
    - Resolves data quality issues (NULL handling, invalid values)
    - Converts raw date fields into proper DATE data types
    - Applies business rules for:
        • Customer deduplication
        • Gender and marital status standardization
        • Product hierarchy derivation
        • Sales amount and price validation
        • Country and code normalization
    - Uses window functions to retain latest records where applicable
    - Measures and logs load duration per table and for the full batch

Source & Target:
    - Source Tables : Bronze schema (raw ingested data)
    - Target Tables : Silver schema (cleaned and conformed data)

Design Notes:
    - No aggregations are performed in the Silver layer
    - Data is reshaped and validated, not summarized
    - This layer acts as the foundation for Gold (business-ready) models

Execution:
    EXEC silver.load_silver;

Layer Context:
    Bronze (Raw Data)
        → Silver (Cleaned & Conformed Data)
            → Gold (Business & Analytics Ready)

===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
	DECLARE 
		@start_time DATETIME, 
		@end_time DATETIME, 
		@batch_start_time DATETIME, 
		@batch_end_time DATETIME
	
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		
		PRINT('========================================')
		PRINT('Loading the silver Layer')
		PRINT('========================================')

		PRINT('****************************************')
		PRINT('Loading CRM Tables')
		PRINT('****************************************')

		SET @start_time = GETDATE()
		PRINT ('Truncating the table - silver.crm_cust_info')
		TRUNCATE TABLE silver.crm_cust_info
		PRINT ('Inserting Data into the table - silver.crm_cust_info')
		INSERT INTO silver.crm_cust_info
		(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
				WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			END AS cst_marital_status,
			CASE 
				WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
				WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gender,
			cst_create_date
		FROM
			(
				SELECT 
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL
			)t
		WHERE flag = 1;
		SET @end_time = GETDATE()
		PRINT('Loading Time: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds')
		PRINT('----------------------------------------')

		SET @start_time = GETDATE()
		PRINT ('Truncating the table - silver.crm_prd_info')
		TRUNCATE TABLE silver.crm_prd_info
		PRINT ('Inserting Data into the table - silver.crm_prd_info')
		INSERT INTO silver.crm_prd_info
		(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			  prd_id,
			  REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			  SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			  prd_nm,
			  COALESCE(prd_cost,0) AS prd_cost,
			  CASE
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			  END AS prd_line,
			  CAST(prd_start_dt AS DATE) AS prd_start_dt,
			  CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE()
		PRINT('Loading Time: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds')
		PRINT('----------------------------------------')

		SET @start_time = GETDATE()
		PRINT ('Truncating the table - silver.crm_sales_details')
		TRUNCATE TABLE silver.crm_sales_details
		PRINT ('Inserting Data into the table - silver.crm_sales_details')
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE
				WHEN sls_order_dt =0 OR LEN(sls_order_dt) != 8 
				THEN NULL
				else CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt, 
			CASE
				WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) != 8 
				THEN NULL
				else CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt, 
				CASE
				WHEN sls_due_dt =0 OR LEN(sls_due_dt) != 8 
				THEN NULL
				else CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt, 
				CASE
				WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
				CASE
				WHEN sls_price <=0 OR sls_price IS NULL 
				THEN  sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;
		PRINT('Loading Time: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds')

		PRINT('****************************************')
		PRINT('Loading ERP Tables')
		PRINT('****************************************')

		SET @start_time = GETDATE()
		PRINT ('Truncating the table - silver.erp_cust_az12')
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT ('Inserting Data into the table - silver.erp_cust_az12')
		INSERT INTO silver.erp_cust_az12 
		(	
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
				ELSE cid
			END AS cid,
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
				CASE
				WHEN TRIM(UPPER(gen)) IN ('M','MALE') THEN 'Male'
				WHEN TRIM(UPPER(gen)) IN ('F','FEMALE') THEN 'Female'
				ELSE 'n/a'
			END AS gen
		FROM bronze.erp_cust_az12
		PRINT('Loading Time: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds')
		PRINT('----------------------------------------')

		SET @start_time = GETDATE()
		PRINT ('Truncating the table - silver.erp_loc_a101')
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT ('Inserting Data into the table - silver.erp_loc_a101')
		INSERT INTO silver.erp_loc_a101
		(	
			cid,
			cntry
		)
		SELECT 
			REPLACE(cid,'-','') cid,
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE cntry
			END AS cntry
		FROM bronze.erp_loc_a101 
		PRINT('Loading Time: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds')
		PRINT('----------------------------------------')

		SET @start_time = GETDATE()
		PRINT ('Truncating the table - silver.erp_px_cat_g1v2')
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT ('Inserting Data into the table - silver.erp_px_cat_g1v2')
		INSERT INTO silver.erp_px_cat_g1v2
		(	
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()
		PRINT('Loading Time: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds')
		PRINT('----------------------------------------')

		SET @batch_end_time = GETDATE()
		PRINT('loading the silver layer completed')
		PRINT('Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time, @batch_end_time) AS VARCHAR)+ ' seconds')
		PRINT('----------------------------------------')
	END TRY

	BEGIN CATCH
		-- Basic error visibility for ETL troubleshooting
		PRINT('----------------------------------------')
		PRINT ('Error Occured during loading bronze layer')
		PRINT ('Error Message' + ERROR_MESSAGE())
		PRINT ('Error Number' + CAST(ERROR_NUMBER() AS VARCHAR))
		PRINT('----------------------------------------')
	END CATCH

END
