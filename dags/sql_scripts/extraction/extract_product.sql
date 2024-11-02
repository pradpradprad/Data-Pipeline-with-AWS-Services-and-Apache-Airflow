SELECT * FROM aws_s3.query_export_to_s3(
    $$
    SELECT
        p.product_id,
        p.product_name,
        b.brand_name,
        c.category_name,
        p.model_year,
        p.list_price,
        p.updated_at,
        p.is_active
    FROM store.products as p
    JOIN store.brands as b
        ON p.brand_id = b.brand_id
    JOIN store.categories as c
        ON p.category_id = c.category_id
    WHERE p.updated_at > '{{ ti.xcom_pull(task_ids="get_etl_date.previous_etl_date") }}'
        AND p.updated_at <= '{{ ti.xcom_pull(task_ids="get_etl_date.current_etl_date") }}';
    $$,
    aws_commons.create_s3_uri(
        '{{ params.bucket }}',
        '{{ params.file_path }}/{{ params.file_name }}-products.csv',
        'ap-southeast-1'
    ),
    options := 'format csv, header true'
);