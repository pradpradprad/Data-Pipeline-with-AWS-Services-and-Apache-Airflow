# begin transaction
begin = 'BEGIN TRANSACTION;'


# insert records from staging table to dimension table
# where
#     1) product_id in dimension table is not present (new record) or
#     2) record already exists in dimension table, but has update on columns
insert = '''
INSERT INTO serving.dim_products (
    product_id, product_name, brand_name, category_name,
    model_year, list_price, eff_date, exp_date, active_flag
)
SELECT
    stg.product_id, stg.product_name, stg.brand_name, stg.category_name,
    stg.model_year, stg.list_price, stg.eff_date, '9999-12-31', 'Y'
FROM staging.stg_products AS stg
LEFT JOIN serving.dim_products AS dim
    ON stg.product_id = dim.product_id
WHERE dim.product_id IS NULL
    OR (
        dim.active_flag = 'Y'
        AND (
            dim.product_name != stg.product_name OR
            dim.brand_name != stg.brand_name OR
            dim.category_name != stg.category_name OR
            dim.model_year != stg.model_year OR
            dim.list_price != stg.list_price
        )
    );
'''


# update expiration date and active flag for updated records in dimension table
# by comparing with records in staging table
# where
#     1) both tables have record with same product_id and
#     2) record in dimension table is currently active and
#     3) have difference in some columns when compare between 2 tables
update = '''
UPDATE serving.dim_products
SET
    exp_date = stg.eff_date - INTERVAL '1 day',
    active_flag = 'N'
FROM staging.stg_products AS stg
WHERE serving.dim_products.product_id = stg.product_id
    AND serving.dim_products.active_flag = 'Y'
    AND (
        serving.dim_products.product_name != stg.product_name OR
        serving.dim_products.brand_name != stg.brand_name OR
        serving.dim_products.category_name != stg.category_name OR
        serving.dim_products.model_year != stg.model_year OR
        serving.dim_products.list_price != stg.list_price
    );
'''


# update expiration date and active flag for deleted records in dimension table
# by comparing with records in staging table
# where
#     1) both tables have record with same product_id and
#     2) record in dimension table is currently active and
#     3) record in staging table is inactive
delete = '''
UPDATE serving.dim_products
SET
    exp_date = stg.eff_date,
    active_flag = 'N'
FROM staging.stg_products AS stg
WHERE serving.dim_products.product_id = stg.product_id
    AND serving.dim_products.active_flag = 'Y'
    AND stg.active_flag = 'N';
'''


# commit transaction
commit = 'COMMIT TRANSACTION;'


dim_products = [begin, insert, update, delete, commit]