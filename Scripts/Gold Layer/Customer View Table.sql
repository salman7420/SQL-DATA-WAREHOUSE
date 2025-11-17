/*Three tables are related to customer: crm_cust_info,cust_az12, loc_a101 
so we will join them and select the column that we need
 */


CREATE OR ALTER VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- system generated primary id called surrogate key -- it has no meaning, created just for data modelling
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS cst_marital_status,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
			ELSE COALESCE(ca.gen,'n/a')
END AS gender,
ca.bdate AS birth_date,
ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12  ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON la.cid = ci.cst_key;

