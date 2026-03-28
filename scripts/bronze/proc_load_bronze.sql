/*
Stored procedure: Load Bronze Layer(Source --> Bronze)

Purpose:
     This stored procedure Loads Data into the 'Bronze' schema from external CSV files.
    Actions performed:
          -Truncates tables before loading
          -Uses BULK INSERT command to load data from csv files to bronze tables.

Usage example:
    EXEC bronze.load_bronze;
============================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @end_batch_time DATETIME;

	BEGIN TRY
		PRINT '=====================' + CHAR(13) + 'Loading Bronze Layer' + CHAR(13) + '=====================' + CHAR(13) + '=====================' + CHAR(13) + 'Loading CRM Tables' + CHAR(13) + '=====================';
		SET @start_batch_time = GETDATE();
		-- 1. Load customer info
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\abhay\Downloads\Data warehousing\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

		-- 2. Load Product info

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\abhay\Downloads\Data warehousing\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds';


		-- 3. Load Sales Details
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\abhay\Downloads\Data warehousing\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

				PRINT '=====================' + CHAR(13) + 'Loading ERP Tables' + CHAR(13) + '=====================';

		SET @start_time = GETDATE();
		-- 4. Load ERP Customer AZ12
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\abhay\Downloads\Data warehousing\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';



		-- 5. Load ERP location Table A101
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\abhay\Downloads\Data warehousing\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';


		-- 6. Load ERP Product Category Table G1V2'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\abhay\Downloads\Data warehousing\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		SET @end_batch_time = GETDATE();
		PRINT 'Load Duration of complete bronze layer: ' + CAST(DATEDIFF(second, @start_batch_time, @end_batch_time) AS NVARCHAR) + 'seconds';

		END TRY
	BEGIN CATCH
		PRINT '===================================='
		PRINT 'ERROR OCCURED DURING BROZE LAYER LOADING'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===================================='
	END CATCH
END

