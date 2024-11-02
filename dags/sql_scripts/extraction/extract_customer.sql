SELECT * FROM aws_s3.query_export_to_s3(
    $$
    SELECT
        customer_id,
        first_name,
        last_name,
        phone,
        email,
        street,
        city,
        state,
        zip_code,
        updated_at,
        is_active
    FROM store.customers
    WHERE updated_at > '{{ ti.xcom_pull(task_ids="get_etl_date.previous_etl_date") }}'
        AND updated_at <= '{{ ti.xcom_pull(task_ids="get_etl_date.current_etl_date") }}';
    $$,
    aws_commons.create_s3_uri(
        '{{ params.bucket }}',
        '{{ params.file_path }}/{{ params.file_name }}-customers.csv',
        'ap-southeast-1'
    ),
    options := 'format csv, header true'
);