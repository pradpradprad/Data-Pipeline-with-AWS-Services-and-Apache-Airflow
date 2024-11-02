# begin transaction
begin = 'BEGIN TRANSACTION;'


# insert new records from staging table to fact table
# by joining dimension table to get surrogate key
# where
#     1) order_id in fact table is not present (new record)
insert = '''
INSERT INTO serving.fact_sales (
    order_id, product_key, customer_key, store_key, staff_key, order_date,
    required_date, shipped_date, rejected_date, order_status_id, quantity,
    discount_percent, discount_amount, total_price, total_price_with_discount
)
SELECT
    stg.order_id, pd.product_key, ct.customer_key, st.store_key, sf.staff_key, stg.order_date,
    stg.required_date, stg.shipped_date, stg.rejected_date, os.order_status_id, stg.quantity,
    stg.discount_percent, stg.discount_amount, stg.total_price, stg.total_price_with_discount
FROM staging.stg_sales AS stg
JOIN serving.dim_customers AS ct
    ON stg.customer_id = ct.customer_id
    AND TO_DATE(stg.order_date, 'YYYYMMDD') BETWEEN ct.eff_date AND ct.exp_date
JOIN serving.dim_stores AS st
    ON stg.store_id = st.store_id
    AND TO_DATE(stg.order_date, 'YYYYMMDD') BETWEEN st.eff_date AND st.exp_date
JOIN serving.dim_order_status AS os
    ON stg.order_status = os.order_status
JOIN serving.dim_staffs AS sf
    ON stg.staff_id = sf.staff_id
    AND TO_DATE(stg.order_date, 'YYYYMMDD') BETWEEN sf.eff_date AND sf.exp_date
JOIN serving.dim_products AS pd
	ON stg.product_id = pd.product_id
	AND TO_DATE(stg.order_date, 'YYYYMMDD') BETWEEN pd.eff_date AND pd.exp_date
LEFT JOIN serving.fact_sales AS sa
	ON stg.order_id = sa.order_id
WHERE sa.order_id IS NULL;
'''


# update date and status columns on existing records in fact table
# by comparing with records in staging table
# where
#     1) both tables have record with same order_id and
#     2) have difference in some columns when compare between 2 tables
#         (fill null when compare between 2 tables)
update = '''
UPDATE serving.fact_sales
SET
    shipped_date = stg.shipped_date,
	rejected_date = stg.rejected_date,
	order_status_id = os.order_status_id
FROM staging.stg_sales AS stg, serving.dim_order_status AS os
WHERE serving.fact_sales.order_id = stg.order_id
    AND stg.order_status = os.order_status
	AND (
		COALESCE(serving.fact_sales.shipped_date, '9999-12-31') != COALESCE(stg.shipped_date, '9999-12-31') OR
		COALESCE(serving.fact_sales.rejected_date, '9999-12-31') != COALESCE(stg.rejected_date, '9999-12-31')
	);
'''


# commit transaction
commit = 'COMMIT TRANSACTION;'


fact_sales = [begin, insert, update, commit]