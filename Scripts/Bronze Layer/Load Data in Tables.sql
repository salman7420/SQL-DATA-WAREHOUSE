/*
Query for loading the data into tables. We will be applying the same logic used previously. Will create a stored procedure to store the data into the tables. 
Truncate and Insert Architecture, for full load meaning we will delete all the data in the table and then load the new data all together. 

**** Warning ****
Truncate will delete all the data in the table, so be careful before using this query!
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;

	
	BEGIN TRY
	SET @batch_start = GETDATE();
		PRINT 'Loading Bronze Layer';
		PRINT '*********************************************';

		PRINT 'Loading Data From CRM Source';
		PRINT '*********************************************';

		SET @start_time = GETDATE();
		PRINT 'Truncating bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT 'Loading bronze.crm_cust_info';
		PRINT '*********************************************';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\user\Desktop\Datawarehouse Project\Data\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT 'Duration to Load cust_info:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds'; 

		SET @start_time = GETDATE();
		PRINT 'Truncating bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT 'Loading bronze.crm_prd_info';
		PRINT '*********************************************';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\user\Desktop\Datawarehouse Project\Data\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT 'Duration to Load crm_prd_info:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		PRINT 'Truncating bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT 'Loading bronze.crm_sales_details';
		PRINT '*********************************************';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\user\Desktop\Datawarehouse Project\Data\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Duration to Load crm_sales_details:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';

		PRINT 'Loading Data From ERP Source';

		SET @start_time = GETDATE();

		PRINT 'Truncating bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT 'Loading bronze.erp_cust_az12';
		PRINT '*********************************************';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\user\Desktop\Datawarehouse Project\Data\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Duration to Load erp_cust_az12:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';


		SET @start_time = GETDATE();
		PRINT 'Truncating bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT 'Loading bronze.erp_loc_a101';
		PRINT '*********************************************';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\user\Desktop\Datawarehouse Project\Data\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Duration to Load erp_loc_a101:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();

		PRINT 'Truncating bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT 'Loading bronze.erp_px_cat_g1v2';
		PRINT '*********************************************';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\user\Desktop\Datawarehouse Project\Data\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT 'Duration to Load erp_px_cat_g1v2:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';

		SET @batch_end = GETDATE();
		PRINT 'TOTAL DURATION TO LOAD BRONZE LAYER:' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS NVARCHAR) + 'seconds'
		END TRY

		BEGIN CATCH 
			PRINT '===================================================================';
			PRINT 'Error Occured During Bronze Layer Loading';
			PRINT 'Error:' + ERROR_MESSAGE();
		END CATCH;
END;

-- Execute the bronze.load_bronze procedure
EXEC bronze.load_bronze;