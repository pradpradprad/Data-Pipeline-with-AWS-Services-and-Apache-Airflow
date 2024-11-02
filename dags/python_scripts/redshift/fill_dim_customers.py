# begin transaction
begin = 'BEGIN TRANSACTION;'


# insert records from staging table to dimension table
# where
#     1) customer_id in dimension table is not present (new record) or
#     2) record already exists in dimension table, but has update on columns
#         (coalesce null in necessary columns when compare between 2 tables)
insert = '''
INSERT INTO serving.dim_customers (
    customer_id, first_name, last_name, phone, email, street,
    city, state, zip_code, eff_date, exp_date, active_flag
)
SELECT
    stg.customer_id, stg.first_name, stg.last_name, stg.phone, stg.email, stg.street,
    stg.city, stg.state, stg.zip_code, stg.eff_date, '9999-12-31', 'Y'
FROM staging.stg_customers AS stg
LEFT JOIN serving.dim_customers AS dim
    ON stg.customer_id = dim.customer_id
WHERE dim.customer_id IS NULL
    OR (
        dim.active_flag = 'Y'
        AND (
            dim.first_name != stg.first_name OR
            dim.last_name != stg.last_name OR
            COALESCE(dim.phone, '') != COALESCE(stg.phone, '') OR
            COALESCE(dim.email, '') != COALESCE(stg.email, '') OR
            COALESCE(dim.street, '') != COALESCE(stg.street, '') OR
            dim.city != stg.city OR
            dim.state != stg.state OR
            dim.zip_code != stg.zip_code
        )
    );
'''


# update expiration date and active flag for updated records in dimension table
# by comparing with records in staging table
# where
#     1) both tables have record with same customer_id and
#     2) record in dimension table is currently active and
#     3) have difference in some columns when compare between 2 tables
#         (coalesce null in necessary columns when compare between 2 tables)
update = '''
UPDATE serving.dim_customers
SET
    exp_date = stg.eff_date - INTERVAL '1 day',
    active_flag = 'N'
FROM staging.stg_customers AS stg
WHERE serving.dim_customers.customer_id = stg.customer_id
    AND serving.dim_customers.active_flag = 'Y'
    AND (
        serving.dim_customers.first_name != stg.first_name OR
        serving.dim_customers.last_name != stg.last_name OR
        COALESCE(serving.dim_customers.phone, '') != COALESCE(stg.phone, '') OR
        COALESCE(serving.dim_customers.email, '') != COALESCE(stg.email, '') OR
        COALESCE(serving.dim_customers.street, '') != COALESCE(stg.street, '') OR
        serving.dim_customers.city != stg.city OR
        serving.dim_customers.state != stg.state OR
        serving.dim_customers.zip_code != stg.zip_code
    );
'''


# update expiration date and active flag for deleted records in dimension table
# by comparing with records in staging table
# where
#     1) both tables have record with same customer_id and
#     2) record in dimension table is currently active and
#     3) record in staging table is inactive
delete = '''
UPDATE serving.dim_customers
SET
    exp_date = stg.eff_date,
    active_flag = 'N'
FROM staging.stg_customers AS stg
WHERE serving.dim_customers.customer_id = stg.customer_id
    AND serving.dim_customers.active_flag = 'Y'
    AND stg.active_flag = 'N';
'''


# commit transaction
commit = 'COMMIT TRANSACTION;'


dim_customers = [begin, insert, update, delete, commit]