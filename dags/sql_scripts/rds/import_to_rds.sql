-- Main table

-- stores table
SELECT aws_s3.table_import_from_s3(
    'store.stores',                     -- RDS table name
    '',                                 -- selected columns
    '(format csv, header true)',        -- format and header
    aws_commons.create_s3_uri(
        '{{ params.bucket_name }}',     -- s3 bucket name
        'input/stores.csv',             -- file path
        'ap-southeast-1'                -- region
    )
);

-- staffs table
SELECT aws_s3.table_import_from_s3(
    'store.staffs',
    '',
    '(format csv, header true)',
    aws_commons.create_s3_uri(
        '{{ params.bucket_name }}',
        'input/staffs.csv',
        'ap-southeast-1'
    )
);

-- categories table
SELECT aws_s3.table_import_from_s3(
    'store.categories',
    '',
    '(format csv, header true)',
    aws_commons.create_s3_uri(
        '{{ params.bucket_name }}',
        'input/categories.csv',
        'ap-southeast-1'
    )
);

-- brands table
SELECT aws_s3.table_import_from_s3(
    'store.brands',
    '',
    '(format csv, header true)',
    aws_commons.create_s3_uri(
        '{{ params.bucket_name }}',
        'input/brands.csv',
        'ap-southeast-1'
    )
);

-- products table
SELECT aws_s3.table_import_from_s3(
    'store.products',
    '',
    '(format csv, header true)',
    aws_commons.create_s3_uri(
        '{{ params.bucket_name }}',
        'input/products.csv',
        'ap-southeast-1'
    )
);

-- customers table
SELECT aws_s3.table_import_from_s3(
    'store.customers',
    '',
    '(format csv, header true)',
    aws_commons.create_s3_uri(
        '{{ params.bucket_name }}',
        'input/customers.csv',
        'ap-southeast-1'
    )
);

-- order_status table
SELECT aws_s3.table_import_from_s3(
    'store.order_status',
    '',
    '(format csv, header true)',
    aws_commons.create_s3_uri(
        '{{ params.bucket_name }}',
        'input/order_status.csv',
        'ap-southeast-1'
    )
);

-- orders table
SELECT aws_s3.table_import_from_s3(
    'store.orders',
    '',
    '(format csv, header true)',
    aws_commons.create_s3_uri(
        '{{ params.bucket_name }}',
        'input/orders.csv',
        'ap-southeast-1'
    )
);

-- order_items table
SELECT aws_s3.table_import_from_s3(
    'store.order_items',
    '',
    '(format csv, header true)',
    aws_commons.create_s3_uri(
        '{{ params.bucket_name }}',
        'input/order_items.csv',
        'ap-southeast-1'
    )
);

-- stocks table
SELECT aws_s3.table_import_from_s3(
    'store.stocks',
    '',
    '(format csv, header true)',
    aws_commons.create_s3_uri(
        '{{ params.bucket_name }}',
        'input/stocks.csv',
        'ap-southeast-1'
    )
);