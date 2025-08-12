/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/




if object_id ('silver.crm_cust_info', 'U') IS NOT NULL
	drop table silver.crm_cust_info;

create table silver.crm_cust_info(
cst_id INT,
cst_key varchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date datetime2 default getdate()
);

if object_id ('silver.crm_prd_info', 'U') IS NOT NULL
	drop table silver.crm_prd_info;

create table silver.crm_prd_info(
prd_id int,
cat_id nvarchar(50),
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()
);

if object_id ('silver.crm_sales_details', 'U') IS NOT NULL
	drop table silver.crm_sales_details;

create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate()
);

if object_id ('silver.erp_loc_a101', 'U') IS NOT NULL
	drop table silver.erp_loc_a101;

create table silver.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50),
dwh_create_date datetime2 default getdate()
);

if object_id ('silver.erp_cust_az12', 'U') IS NOT NULL
	drop table silver.erp_cust_az12;

create table silver.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50),
dwh_create_date datetime2 default getdate()
);

if object_id ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	drop table silver.erp_px_cat_g1v2;

create table silver.erp_px_cat_g1v2(
id varchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50),
dwh_create_date datetime2 default getdate()
);


