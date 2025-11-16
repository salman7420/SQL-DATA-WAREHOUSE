-- crm_sales_deails 

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

CASE WHEN sls_price IS NULL OR sls_price != 0
THEN sls_sales / NULLIF(sls_quantity,0)
ELSE sls_price
END AS sls_price,

CASE WHEN sls_sales != ABS(sls_price) * sls_quantity OR sls_sales IS NULL OR sls_sales <= 0
THEN sls_quantity * ABS(sls_price)
ELSE sls_sales
END sls_sales

FROM bronze.crm_sales_details; 

-- Check invalid order date / we are getting date as 0 which is invalid so replace 0 with null
SELECT NULLIF(sls_order_dt,0)
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0;

-- Check the type ---> integer and we need to change to Date
-- Pattern: Year(4)/Month(2)/Year(2) in the int data. and length should be 8
SELECT NULLIF(sls_order_dt,0)
FROM bronze.crm_sales_details
WHERE len(sls_order_dt) != 8 OR 
sls_order_dt <= 0;

-- ship date shouldnt be before/less than order dt
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt;

-- sls_due_dt

SELECT * 
FROM bronze.crm_sales_details
WHERE LEN(sls_due_dt) != 8;

-- CHECK VALID sls_sales, sls_quantity, sls_price
-- sls_sales = sls_quantity * sls_price
SELECT DISTINCT
sls_quantity, sls_price, sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,sls_price,sls_quantity;

-- check if quantity is 0 or less
SELECT sls_quantity
FROM bronze.crm_sales_details
WHERE sls_quantity IS NULL;

-- bronze.erp_cust_az12 -- cid and cst_key should match for joining!

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
	WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'FEMALE'
	WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'MALE'
	ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12;

SELECT CASE WHEN cid LIKE 'NAS%' 
THEN SUBSTRING(cid, 4, LEN(cid))
ELSE cid
END cid
FROM bronze.erp_cust_az12;

SELECT DISTINCT cst_key
FROM silver.crm_cust_info;
  
SELECT *
FROM bronze.erp_cust_az12;

-- check for extreme values of bdate (for eg in future)
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE()
ORDER BY bdate;

-- check gender column
SELECT DISTINCT CASE 
	WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'FEMALE'
	WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'MALE'
	ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12;

SELECT *
FROM bronze.erp_cust_az12;


SELECT *
FROM silver.erp_cust_az12;
-- erp_loc_a101

-- cid contains '-' in the unique id which we dont want so we will remove it.
SELECT 
REPLACE(cid,'-','') AS cid,
CASE 
	WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
ELSE TRIM(cntry) 
END AS cntry
FROM bronze.erp_loc_a101;

-- removing '-' in cid
SELECT 
REPLACE(cid,'-','') AS cid,
cntry
FROM bronze.erp_loc_a101;

-- check valid cntry
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101;


-- erp_px_cat_g1v2

SELECT id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

-- id contains '_- instead of '-'



SELECT  id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (
SELECT cat_id
FROM silver.crm_prd_info);

-- check unwanted space in cat, subcat, and maintenance
-- no unwanted space found!
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance) 

-- check data consistency
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;