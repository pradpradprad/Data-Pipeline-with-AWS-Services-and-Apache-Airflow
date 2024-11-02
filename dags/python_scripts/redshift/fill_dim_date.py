# begin transaction
begin = 'BEGIN TRANSACTION;'


# insert records from staging table to dimension table
# where
#     1) date_key in dimension table is not present (new record)
insert = '''
INSERT INTO serving.dim_date (
    date_key, date, year, month, day,
    quarter, day_of_week, month_name, week_of_year
)
SELECT
    stg.date_key, stg.date, stg.year, stg.month, stg.day,
    stg.quarter, stg.day_of_week, stg.month_name, stg.week_of_year
FROM staging.stg_date AS stg
LEFT JOIN serving.dim_date AS dim
    ON stg.date_key = dim.date_key
WHERE dim.date_key IS NULL;
'''


# commit transaction
commit = 'COMMIT TRANSACTION;'


dim_date = [begin, insert, commit]