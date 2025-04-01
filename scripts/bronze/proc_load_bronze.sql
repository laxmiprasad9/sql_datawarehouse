--------
loads the data from csv into table
--------
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT '=====================';
	PRINT 'LOADING BRONZE LAYER';
	PRINT '=====================';
	PRINT '---------------------';
	PRINT 'Loading CRM tables';
	PRINT '---------------------';
	SET @start_time = GETDATE();
	PRINT 'Truncating table:bronze.crm_cust_info';
TRUNCATE TABLE bronze.crm_cust_info;
PRINT 'Inserting:bronze.crm_cust_info';
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\Chintu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
	SET @end_time = GETDATE();
	PRINT '** LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + 'seconds';
	PRINT '------------------------------------------------------------------------------------------'
	SET @start_time = GETDATE();

PRINT 'Truncating:bronze.crm_prd_info';
TRUNCATE TABLE bronze.crm_prd_info;
PRINT 'Inserting:bronze.crm_prd_info';
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\Chintu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
	SET @end_time = GETDATE();
	PRINT '** LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + 'seconds';
	PRINT '------------------------------------------------------------------------------------------'
	SET @start_time = GETDATE();
PRINT 'Truncating:bronze.crm_sales_details';
TRUNCATE TABLE bronze.crm_sales_details;
PRINT 'Inserting:bronze.crm_sales_details';
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\Chintu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
	SET @end_time = GETDATE();
	PRINT '** LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + 'seconds';
	PRINT '------------------------------------------------------------------------------------------'
	SET @start_time = GETDATE();
	PRINT '---------------------';
	PRINT 'Loading ERP tables';
	PRINT '---------------------';
	PRINT 'Truncating:bronze.erp_cust_az12';
TRUNCATE TABLE bronze.erp_cust_az12;
PRINT 'Inserting:bronze.erp_cust_az12';
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\Chintu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
	SET @end_time = GETDATE();
	PRINT '** LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + 'seconds';
	PRINT '------------------------------------------------------------------------------------------'
	SET @start_time = GETDATE();
PRINT 'Truncating:bronze.erp_loc_a101';
TRUNCATE TABLE bronze.erp_loc_a101;
PRINT 'Inserting:bronze.erp_loc_a101';
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\Chintu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
	SET @end_time = GETDATE();
	PRINT '** LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + 'seconds';
	PRINT '------------------------------------------------------------------------------------------'
	SET @start_time = GETDATE();
PRINT 'Truncating:bronze.erp_px_cat_g1v2';
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
PRINT 'Inserting:bronze.erp_px_cat_g1v2';
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\Chintu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
	SET @end_time = GETDATE();
	PRINT '** LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + 'seconds';
	PRINT '------------------------------------------------------------------------------------------'
	SET @batch_end_time = GETDATE();
	PRINT '** LOAD DURATION OF bRONZE LAYER :'+ CAST(DATEDIFF(second,@start_time, @end_time)AS NVARCHAR) + 'seconds';
	PRINT '------------------------------------------------------------------------------------------'
	END TRY
	BEGIN CATCH
	PRINT'=================================================================================';
	PRINT'ERROR OCCURED WHILE LOADING BRONZE';
	PRINT'ERROR MESSAGE'+ ERROR_MESSAGE();
	PRINT'ERROR MESSAGE'+ CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT'ERROR MESSAGE'+ CAST (ERROR_STATE() AS NVARCHAR);
	PRINT'=================================================================================';
	END CATCH
END
