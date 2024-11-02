# begin transaction
begin = 'BEGIN TRANSACTION;'


# load data from s3 to staging tables

load_stg_customers = '''
COPY staging.stg_customers
FROM 's3://{{ params.bucket }}/{{ params.path }}/customers/part-'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;
'''

load_stg_stores = '''
COPY staging.stg_stores
FROM 's3://{{ params.bucket }}/{{ params.path }}/stores/part-'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;
'''

load_stg_staffs = '''
COPY staging.stg_staffs
FROM 's3://{{ params.bucket }}/{{ params.path }}/staffs/part-'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;
'''

load_stg_date = '''
COPY staging.stg_date
FROM 's3://{{ params.bucket }}/{{ params.path }}/date/part-'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;
'''

load_stg_products = '''
COPY staging.stg_products
FROM 's3://{{ params.bucket }}/{{ params.path }}/products/part-'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;
'''

load_stg_sales = '''
COPY staging.stg_sales
FROM 's3://{{ params.bucket }}/{{ params.path }}/sales/part-'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;
'''


# commit transaction
commit = 'COMMIT TRANSACTION;'


load_to_staging = [
    begin,
    load_stg_customers, load_stg_stores, load_stg_staffs,
    load_stg_date, load_stg_products, load_stg_sales,
    commit
]

clear_staging_tables = [
    begin,
    'TRUNCATE staging.stg_customers;',
    'TRUNCATE staging.stg_date;',
    'TRUNCATE staging.stg_products;',
    'TRUNCATE staging.stg_staffs;',
    'TRUNCATE staging.stg_stores;',
    'TRUNCATE staging.stg_sales;',
    commit
]