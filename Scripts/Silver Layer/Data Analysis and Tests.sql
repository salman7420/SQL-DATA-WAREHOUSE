/*
Analysis of bronze layer tables to see what cleaning is required in the silver layer! Once we have determined the inconsistency with the data,
we will create a query to solve that and then load the data to the silver layer tables.
*/

SELECT cst_id, Count(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL; -- to find if unique id contains duplicate


SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466; -- analyzing one value that is duplicate (shouldnt be since this is unique id)

-- Sort data by date, and extract the latest row:
-- Check where flag_last is more than one meaning those ids are duplicate:

SELECT  *
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info) t
WHERE flag_last = 1;  -- This query returns all the ids and its data that is not duplicate!

-- Check unwanted spaces:
-- Expections is No results, if any name returned meaning they have unwanted space thats need to be removed!
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Analysis of gender and maritalstatus column
-- Instead of 'F' and 'M' we want Female and Male and same cosistency in marital status column


-- QUERY TO CLEAN DATA, AND ALL OTHER INCONSISTENCIES.

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

-- QUERY T0 EXTRACT Cleaned Data From bronze.crm_cust_info

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


---- bronze.crm_prd_info
SELECT prd_id, Count(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Extracting cat_id
SELECT REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS cat_id
FROM bronze.crm_prd_info;

-- in erp prd_key contains "_" and in crm id contains '-' so use Replace to make the id consistent
SELECT id
FROM bronze.erp_px_cat_g1v2;

-- Finding what category id is not in both crm and erp

SELECT *, REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS cat_id
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1,5),'-','_') NOT IN (
SELECT DISTINCT(id)
FROM bronze.erp_px_cat_g1v2);

SELECT DISTINCT(id)
FROM bronze.erp_px_cat_g1v2;

-- extract prd_key 
SELECT *,
REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info;

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

-- Handling Data inconsistency

SELECT 
prd_id,
prd_key,
prd_nm,	
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC0HE-HL-U509-R', 'AC-HE-HL-U509');