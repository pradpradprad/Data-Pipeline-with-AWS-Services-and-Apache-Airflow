import numpy as np
import pandas as pd


def modify_customer_table(df_customers: pd.DataFrame, df_orders: pd.DataFrame) -> pd.DataFrame:
    """
    add audit columns to existing dataframe

    :param df_customers: existing customer dataframe
    :param df_orders: existing order dataframe
    """
    
    # merge with order table
    df_customers = df_orders.merge(df_customers, how='inner', on='customer_id')
    
    # change data type to datetime format
    df_customers['order_date'] = pd.to_datetime(df_customers['order_date'])
    df_customers['required_date'] = pd.to_datetime(df_customers['required_date'])
    df_customers['shipped_date'] = pd.to_datetime(df_customers['shipped_date'])
    
    # get minimum order date of each customer to use it as create date
    df_min_order_date = df_customers.groupby('customer_id')['order_date'].min().reset_index()
    df_min_order_date = df_min_order_date.rename(columns={'order_date': 'created_at'})
    
    # merge to get create date column and add update date column
    # assume the latest update is when customers create their account
    df_customers = df_customers.merge(df_min_order_date, how='inner', on='customer_id')
    df_customers['updated_at'] = df_customers['created_at']
    
    # drop duplicates and sort values
    df_customers = df_customers.drop_duplicates(subset='customer_id') \
                            .sort_values('customer_id') \
                            .reset_index()

    # add active status column to customers
    df_customers['is_active'] = 'Y'

    # select columns
    df_customers = df_customers[[
        'customer_id',
        'first_name',
        'last_name',
        'phone',
        'email',
        'street',
        'city',
        'state',
        'zip_code',
        'created_at',
        'updated_at',
        'is_active'
    ]]
    
    return df_customers


def modify_staff_table(df_staffs: pd.DataFrame) -> pd.DataFrame:
    """
    add audit columns to existing dataframe

    :param df_staffs: existing staff dataframe
    """
    
    # fill null and change data type to str
    df_staffs['manager_id'] = df_staffs['manager_id'].fillna(1).astype('int').astype('str')

    # drop old active column
    df_staffs = df_staffs.drop(columns='active')

    # add columns to each staff member
    df_staffs['created_at'] = pd.to_datetime('2016-01-01')
    df_staffs['updated_at'] = pd.to_datetime('2016-01-01')
    df_staffs['is_active'] = 'Y'
    
    return df_staffs


def modify_product_table(df_products: pd.DataFrame) -> pd.DataFrame:
    """
    add audit columns to existing dataframe

    :param df_products: existing product dataframe
    """

    # add columns to each product
    df_products['created_at'] = pd.to_datetime('2016-01-01')
    df_products['updated_at'] = pd.to_datetime('2016-01-01')
    df_products['is_active'] = 'Y'
    
    return df_products


def modify_store_table(df_stores: pd.DataFrame) -> pd.DataFrame:
    """
    add audit columns to existing dataframe

    :param df_stores: existing store dataframe
    """
    
    # add columns to each store
    df_stores['created_at'] = pd.to_datetime('2016-01-01')
    df_stores['updated_at'] = pd.to_datetime('2016-01-01')
    df_stores['is_active'] = 'Y'
    
    return df_stores


def modify_order_table(df_orders: pd.DataFrame) -> pd.DataFrame:
    """
    add audit column to existing dataframe
    
    :param df_orders: existing order dataframe
    """

    # rename column
    df_orders = df_orders.rename(columns={'order_status': 'order_status_id'})

    # change data type to datetime format
    df_orders['order_date'] = pd.to_datetime(df_orders['order_date'])
    df_orders['required_date'] = pd.to_datetime(df_orders['required_date'])
    df_orders['shipped_date'] = pd.to_datetime(df_orders['shipped_date'])

    # change order status by combining 'Processing' status into 'Pending' status
    df_orders['order_status_id'] = df_orders['order_status_id'].replace({
        2: 1,
        3: 2,
        4: 3
    })

    # specify latest update date on order status based on each status
    # assume latest update as shipped date for 'Completed' status
    # assume latest update as 1 day after required date for 'Rejected' status
    # assume latest update as order date for 'Pending' status
    df_orders['updated_at'] = np.where(df_orders['order_status_id']==3, df_orders['shipped_date'],
                                np.where(df_orders['order_status_id']==2, df_orders['required_date'] + pd.Timedelta(days=1),
                                df_orders['order_date']))
    
    return df_orders


def create_order_status_table() -> pd.DataFrame:
    """
    create lookup table for order status
    """

    # create dataframe
    df_order_status = pd.DataFrame({
        'order_status_id': [1,2,3],
        'order_status': ['Pending', 'Rejected', 'Completed']
    })
    
    return df_order_status


def transform_csv(source_bucket: str):
    """
    read, transform and save output to s3 bucket

    :param source_bucket: s3 data source bucket name
    """

    # read csv from s3 bucket
    df_brands = pd.read_csv(f's3://{source_bucket}/dataset/brands.csv')
    df_categories = pd.read_csv(f's3://{source_bucket}/dataset/categories.csv')
    df_customers = pd.read_csv(f's3://{source_bucket}/dataset/customers.csv')
    df_order_items = pd.read_csv(f's3://{source_bucket}/dataset/order_items.csv')
    df_orders = pd.read_csv(f's3://{source_bucket}/dataset/orders.csv')
    df_products = pd.read_csv(f's3://{source_bucket}/dataset/products.csv')
    df_staffs = pd.read_csv(f's3://{source_bucket}/dataset/staffs.csv')
    df_stocks = pd.read_csv(f's3://{source_bucket}/dataset/stocks.csv')
    df_stores = pd.read_csv(f's3://{source_bucket}/dataset/stores.csv')

    # transform/create table
    df_customers = modify_customer_table(df_customers, df_orders)
    df_staffs = modify_staff_table(df_staffs)
    df_products = modify_product_table(df_products)
    df_stores = modify_store_table(df_stores)
    df_orders = modify_order_table(df_orders)
    df_order_status = create_order_status_table()

    # save output to s3 bucket
    df_brands.to_csv(f's3://{source_bucket}/input/brands.csv', index=False)
    df_categories.to_csv(f's3://{source_bucket}/input/categories.csv', index=False)
    df_customers.to_csv(f's3://{source_bucket}/input/customers.csv', index=False)
    df_order_items.to_csv(f's3://{source_bucket}/input/order_items.csv', index=False)
    df_orders.to_csv(f's3://{source_bucket}/input/orders.csv', index=False)
    df_products.to_csv(f's3://{source_bucket}/input/products.csv', index=False)
    df_staffs.to_csv(f's3://{source_bucket}/input/staffs.csv', index=False)
    df_stocks.to_csv(f's3://{source_bucket}/input/stocks.csv', index=False)
    df_stores.to_csv(f's3://{source_bucket}/input/stores.csv', index=False)
    df_order_status.to_csv(f's3://{source_bucket}/input/order_status.csv', index=False)