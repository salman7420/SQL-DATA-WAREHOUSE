--- This file is used for analysis.

-- 2 GENDER COLUMN RETRIEVED FROM QUERY, SO LETS SEE WHICH ONE CONTAINS INACCURATE DATA
-- WE WILL CONSIDER CRM TO BE OUR MASTER DATA SOURCE SO IF CRM AND ERP ARE DIFFERENT VALUES THEN CHOOSE CRM!
SELECT DISTINCT
ci.cst_gndr,
ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12  ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON la.cid = ci.cst_key
ORDER BY 1,2;

-- Rule ---> CRM Master, if null then ERP value.
-- If ERP == NULL then n/a
-- ELSE 'n/a'

SELECT DISTINCT ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
			ELSE COALESCE(ca.gen,'n/a')
END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12  ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON la.cid = ci.cst_key;