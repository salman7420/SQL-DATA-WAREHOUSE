/*
This query is used to load clean data to the silve layer tables.
We will also create a procedure to load all the data into tables of silver layer (cleaned data)!
*/

-- Inserting Data into silver.cust_info:


CREATE OR ALTER PROCEDURE silver.load_silver
AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	PRINT '-------------------------------------------------------';
	PRINT 'Loading Data to Silver Layer';
	PRINT 'Truncating and Loading Data From Source: CRM';
	PRINT '-------------------------------------------------------';
	SET @batch_start = GETDATE();
	SET @start_time = GETDATE();
	PRINT'Truncating silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;

	PRINT 'Inserting to silver.crm_cust_info'
	INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_gndr,
	cst_marital_status,
	cst_create_date
	)
	SELECT  cst_id, 
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM (cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Female' 
		ELSE 'n/a'
	END cst_gndr,

	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
		ELSE 'n/a'
	END cst_marital_status,
	cst_create_date
	FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL) t
	WHERE flag_last = 1;

	SET @end_time = GETDATE();
	PRINT 'Duration to Load silver.crm_cust_info: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
	PRINT '-------------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT'Truncating silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;

	PRINT 'Inserting to silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm ,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	SELECT prd_id,
	REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) as prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END	AS prd_line,	
	prd_start_dt,
	DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
	FROM bronze.crm_prd_info;
	SET @end_time = GETDATE();
	PRINT 'Duration to Load silver.crm_prd_info: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
	PRINT '-------------------------------------------------------';


	SET @start_time = GETDATE();
	PRINT'Truncating silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;

	PRINT 'Inserting to silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt ,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	SELECT sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN LEN(sls_order_dt) != 8 OR sls_order_dt <= 0 THEN NULL 
ELSE CAST( CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,

CASE WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt <= 0 THEN NULL 
ELSE CAST( CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,

CASE WHEN LEN(sls_due_dt) != 8 OR sls_due_dt <= 0 THEN NULL 
ELSE CAST( CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,

sls_quantity,

CASE 
	WHEN sls_price IS NULL OR sls_price != 0
	THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price,

CASE 
	WHEN sls_sales != ABS(sls_price) * sls_quantity OR sls_sales IS NULL OR sls_sales <= 0
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END sls_sales
FROM bronze.crm_sales_details; 

	SET @end_time = GETDATE();
	PRINT 'Duration to Load silver.crm_sales_details: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
	PRINT '-------------------------------------------------------';

	PRINT 'Truncating and Loading Data From Source: ERP';
	PRINT '-------------------------------------------------------';


	SET @start_time = GETDATE();
	PRINT'Truncating silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;

	PRINT 'Inserting to silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
	)
SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END cid,
CASE 
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END bdate,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12;
	

	SET @end_time = GETDATE();
	PRINT 'Duration to Load silver.erp_cust_az12: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds';
	PRINT '-------------------------------------------------------';


SET @start_time = GETDATE();
	PRINT'Truncating silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;

	PRINT 'Inserting to silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 (
	cid,
	cntry
	)
SELECT 
REPLACE(cid,'-','') AS cid,
CASE 
	WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
ELSE TRIM(cntry) 
END AS cntry
FROM bronze.erp_loc_a101;
	

	SET @end_time = GETDATE();
	PRINT 'Duration to Load silver.erp_loc_a101: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '-------------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT'Truncating silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	PRINT 'Inserting to silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
	)
SELECT *
FROM bronze.erp_px_cat_g1v2
	

	SET @end_time = GETDATE();
	PRINT 'Duration to Load silver.erp_px_cat_g1v2: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '-------------------------------------------------------';



	SET @batch_end = GETDATE()
	PRINT 'Duration to Load Data into Silver Layer Tables:: ' + CAST(DATEDIFF(second, @batch_start, @batch_end)AS NVARCHAR) + 'seconds';

END;

EXEC silver.load_silver;