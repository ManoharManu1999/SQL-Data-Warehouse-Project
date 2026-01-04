/*
===============================================================================
Stored Procedure: bronze.load_bronze
===============================================================================

Purpose:
    Loads raw data from external CSV files into the Bronze layer of a
    SQL Server–based Data Warehouse.

    The Bronze layer represents the raw ingestion zone, where data is stored
    exactly as received from source systems (CRM and ERP), without any
    transformations, cleansing, or business logic applied.

What This Procedure Does:
    - Performs a full refresh of Bronze tables using TRUNCATE + BULK INSERT
    - Loads data from CRM and ERP source CSV files
    - Captures row counts for each table load
    - Measures and logs load duration per table and for the full batch
    - Provides execution progress using structured PRINT statements
    - Suppresses default row-count messages using SET NOCOUNT ON

Why This Design:
    - Ensures consistent and repeatable data ingestion
    - Optimised for high-performance bulk loading
    - Separates raw ingestion logic from downstream transformations
    - Acts as the foundation for Silver and Gold layers

Execution Details:
    - Bulk loads are executed using TABLOCK for improved performance
    - All tables are fully truncated before loading (full-refresh strategy)
    - Error handling is implemented using TRY...CATCH blocks

Parameters:
    None.
    This stored procedure does not accept any input parameters.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

	
CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	  -- Suppress default "(x rows affected)" messages for clean ETL logging
    SET NOCOUNT ON;
	
	DECLARE 
		@start_time DATETIME, 
		@end_time DATETIME, 
		@rows INT,
		@batch_start_time DATETIME,
		@batch_end_time DATETIME;

	BEGIN TRY
		-- Track total batch execution time
		SET NOCOUNT ON;
		SET @batch_start_time = GETDATE();
		
		PRINT('****************************************')
		PRINT('Loading the Brozne Layer')
		PRINT('****************************************')

		PRINT('----------------------------------------')
		PRINT('Loading CRM Tables')
		PRINT('----------------------------------------')

			SET @start_time = GETDATE();
			PRINT('Truncating the table bronze.crm_cust_info')
			TRUNCATE TABLE bronze.crm_cust_info
			PRINT('Inserting data into table - bronze.crm_cust_info')
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\Manohar\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				);
			SET @rows = @@ROWCOUNT;
			SET @end_time = GETDATE();	
			PRINT('Inserted rows: ' + CAST(@rows AS VARCHAR));
			PRINT ('Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds')
			PRINT('----------------------------------------')

			SET @start_time = GETDATE()
			PRINT('Truncating the table bronze.crm_prd_info')
			TRUNCATE TABLE bronze.crm_prd_info
			PRINT('Inserting data into table - bronze.crm_prd_info')
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\Manohar\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			)
			SET @rows = @@ROWCOUNT
			SET @end_time = GETDATE()
			PRINT('Inserted rows: ' + CAST(@rows AS VARCHAR));
			PRINT('Loading Time: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds')
			PRINT('----------------------------------------')

			SET @start_time = GETDATE()
			PRINT('Truncating the table bronze.crm_sales_details')
			TRUNCATE TABLE bronze.crm_sales_details
			PRINT('Inserting data into table - bronze.crm_sales_details')
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\Manohar\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			)
			SET @rows = @@ROWCOUNT
			SET @end_time = GETDATE()
			PRINT('Inserted rows: ' + CAST(@rows AS VARCHAR));
			PRINT('Loading Time: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds')

			PRINT('----------------------------------------')
			PRINT('Loading ERP Tables')
			PRINT('----------------------------------------')

			SET @start_time = GETDATE()
			PRINT('Truncating the table bronze.erp_cust_az12')
			TRUNCATE TABLE bronze.erp_cust_az12
			PRINT('Inserting data into table - bronze.erp_cust_az12')
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Users\Manohar\Documents\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			)
			SET @rows = @@ROWCOUNT
			SET @end_time = GETDATE()
			PRINT('Inserted rows: ' + CAST(@rows AS VARCHAR));
			PRINT('Loading Time: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds')
			PRINT('----------------------------------------')

			SET @start_time = GETDATE()
			PRINT('Truncating the table bronze.erp_loc_a101')
			TRUNCATE TABLE bronze.erp_loc_a101
			PRINT('Inserting data into table - bronze.erp_loc_a101')
			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Users\Manohar\Documents\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			)
			SET @rows = @@ROWCOUNT
			SET @end_time = GETDATE()
			PRINT('Inserted rows: ' + CAST(@rows AS VARCHAR));
			PRINT('Loading Time: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds')
			PRINT('----------------------------------------')

			SET @start_time = GETDATE()
			PRINT('Truncating the table bronze.erp_px_cat_g1v2')
			TRUNCATE TABLE bronze.erp_px_cat_g1v2
			PRINT('Inserting data into table - bronze.erp_px_cat_g1v2')
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\Users\Manohar\Documents\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			)
			SET @rows = @@ROWCOUNT
			SET @end_time = GETDATE()
			PRINT('Inserted rows: ' + CAST(@rows AS VARCHAR));
			PRINT('Loading Time: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds')
			PRINT('----------------------------------------')

			SET @batch_end_time = GETDATE()
			PRINT('loading the Bronze layer completed')
			PRINT('Total Load Duration - '+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR)+ ' seconds')
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
