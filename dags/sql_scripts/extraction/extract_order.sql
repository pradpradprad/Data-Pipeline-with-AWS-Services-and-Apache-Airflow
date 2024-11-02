SELECT * FROM aws_s3.query_export_to_s3(
    $$
    SELECT
        o.order_id,
        o.customer_id,
        os.order_status,
        o.order_date,
        o.required_date,
        o.shipped_date,
        o.store_id,
        o.staff_id,
        oi.product_id,
        oi.quantity,
        oi.list_price,
        oi.discount,
        o.updated_at
    FROM store.orders AS o
    JOIN store.order_status AS os
        ON o.order_status_id = os.order_status_id
    JOIN store.order_items AS oi
        ON o.order_id = oi.order_id
    WHERE o.updated_at > '{{ ti.xcom_pull(task_ids="get_etl_date.previous_etl_date") }}'
        AND o.updated_at <= '{{ ti.xcom_pull(task_ids="get_etl_date.current_etl_date") }}';
    $$,
    aws_commons.create_s3_uri(
        '{{ params.bucket }}',
        '{{ params.file_path }}/{{ params.file_name }}-orders.csv',
        'ap-southeast-1'
    ),
    options := 'format csv, header true'
);