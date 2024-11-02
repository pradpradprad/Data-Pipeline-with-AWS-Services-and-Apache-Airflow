# begin transaction
begin = 'BEGIN TRANSACTION;'


# insert records from staging table to dimension table
# where
#     1) staff_id in dimension table is not present (new record) or
#     2) record already exists in dimension table, but has update on columns
insert = '''
INSERT INTO serving.dim_staffs (
    staff_id, first_name, last_name, email, phone,
    manager_id, eff_date, exp_date, active_flag
)
SELECT
    stg.staff_id, stg.first_name, stg.last_name, stg.email, stg.phone,
    stg.manager_id, stg.eff_date, '9999-12-31', 'Y'
FROM staging.stg_staffs AS stg
LEFT JOIN serving.dim_staffs AS dim
    ON stg.staff_id = dim.staff_id
WHERE dim.staff_id IS NULL
    OR (
        dim.active_flag = 'Y'
        AND (
            dim.first_name != stg.first_name OR
            dim.last_name != stg.last_name OR
            dim.email != stg.email OR
            dim.phone != stg.phone OR
            dim.manager_id != stg.manager_id
        )
    );
'''


# update expiration date and active flag for updated records in dimension table
# by comparing with records in staging table
# where
#     1) both tables have record with same staff_id and
#     2) record in dimension table is currently active and
#     3) have difference in some columns when compare between 2 tables
update = '''
UPDATE serving.dim_staffs
SET
    exp_date = stg.eff_date - INTERVAL '1 day',
    active_flag = 'N'
FROM staging.stg_staffs AS stg
WHERE serving.dim_staffs.staff_id = stg.staff_id
    AND serving.dim_staffs.active_flag = 'Y'
    AND (
        serving.dim_staffs.first_name != stg.first_name OR
        serving.dim_staffs.last_name != stg.last_name OR
        serving.dim_staffs.email != stg.email OR
        serving.dim_staffs.phone != stg.phone OR
        serving.dim_staffs.manager_id != stg.manager_id
    );
'''


# update expiration date and active flag for deleted records in dimension table
# by comparing with records in staging table
# where
#     1) both tables have record with same staff_id and
#     2) record in dimension table is currently active and
#     3) record in staging table is inactive
delete = '''
UPDATE serving.dim_staffs
SET
    exp_date = stg.eff_date,
    active_flag = 'N'
FROM staging.stg_staffs AS stg
WHERE serving.dim_staffs.staff_id = stg.staff_id
    AND serving.dim_staffs.active_flag = 'Y'
    AND stg.active_flag = 'N';
'''


# commit transaction
commit = 'COMMIT TRANSACTION;'


dim_staffs = [begin, insert, update, delete, commit]