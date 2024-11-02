SELECT * FROM aws_s3.query_export_to_s3(
    $$
    SELECT
        staff_id,
        first_name,
        last_name,
        email,
        phone,
        manager_id,
        updated_at,
        is_active
    FROM store.staffs
    WHERE updated_at > '{{ ti.xcom_pull(task_ids="get_etl_date.previous_etl_date") }}'
        AND updated_at <= '{{ ti.xcom_pull(task_ids="get_etl_date.current_etl_date") }}';
    $$,
    aws_commons.create_s3_uri(
        '{{ params.bucket }}',
        '{{ params.file_path }}/{{ params.file_name }}-staffs.csv',
        'ap-southeast-1'
    ),
    options := 'format csv, header true'
);