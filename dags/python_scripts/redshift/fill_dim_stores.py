# begin transaction
begin = 'BEGIN TRANSACTION;'


# insert records from staging table to dimension table
# where
#     1) store_id in dimension table is not present (new record) or
#     2) record already exists in dimension table, but has update on columns
insert = '''
INSERT INTO serving.dim_stores (
    store_id, store_name, phone, email, street, city,
    state, zip_code, eff_date, exp_date, active_flag
)
SELECT
    stg.store_id, stg.store_name, stg.phone, stg.email, stg.street, stg.city,
    stg.state, stg.zip_code, stg.eff_date, '9999-12-31', 'Y'
FROM staging.stg_stores AS stg
LEFT JOIN serving.dim_stores AS dim
    ON stg.store_id = dim.store_id
WHERE dim.store_id IS NULL
    OR (
        dim.active_flag = 'Y'
        AND (
            dim.store_name != stg.store_name OR
            dim.phone != stg.phone OR
            dim.email != stg.email OR
            dim.street != stg.street OR
            dim.city != stg.city OR
            dim.state != stg.state OR
            dim.zip_code != stg.zip_code
        )
    );
'''


# update expiration date and active flag for updated records in dimension table
# by comparing with records in staging table
# where
#     1) both tables have record with same store_id and
#     2) record in dimension table is currently active and
#     3) have difference in some columns when compare between 2 tables
update = '''
UPDATE serving.dim_stores
SET
    exp_date = stg.eff_date - INTERVAL '1 day',
    active_flag = 'N'
FROM staging.stg_stores AS stg
WHERE serving.dim_stores.store_id = stg.store_id
    AND serving.dim_stores.active_flag = 'Y'
    AND (
        serving.dim_stores.store_name != stg.store_name OR
        serving.dim_stores.phone != stg.phone OR
        serving.dim_stores.email != stg.email OR
        serving.dim_stores.street != stg.street OR
        serving.dim_stores.city != stg.city OR
        serving.dim_stores.state != stg.state OR
        serving.dim_stores.zip_code != stg.zip_code
    );
'''


# update expiration date and active flag for deleted records in dimension table
# by comparing with records in staging table
# where
#     1) both tables have record with same store_id and
#     2) record in dimension table is currently active and
#     3) record in staging table is inactive
delete = '''
UPDATE serving.dim_stores
SET
    exp_date = stg.eff_date,
    active_flag = 'N'
FROM staging.stg_stores AS stg
WHERE serving.dim_stores.store_id = stg.store_id
    AND serving.dim_stores.active_flag = 'Y'
    AND stg.active_flag = 'N';
'''


# commit transaction
commit = 'COMMIT TRANSACTION;'


dim_stores = [begin, insert, update, delete, commit]