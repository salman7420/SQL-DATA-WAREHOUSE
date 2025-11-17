CREATE OR ALTER VIEW gold.dim_products
AS
SELECT 
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
px.cat AS category,
px.subcat AS subcategory,
px.maintenance,
pn.prd_cost AS product_cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS product_start_date

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 px
ON pn.cat_id = px.id
WHERE prd_end_dt IS NULL; --Keeping only current data (prd not sold)
