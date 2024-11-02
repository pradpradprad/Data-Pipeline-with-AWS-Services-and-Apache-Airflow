from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    date_format, col, year, month, day,
    quarter, weekofyear, when, regexp_replace
)
from pyspark.sql.types import (
    StructType as T, StructField as F,
    StringType, IntegerType, DecimalType, DateType
)
import sys
import logging


# setting log level
logging.basicConfig(level=logging.INFO)


# schema for source tables
customer_schema = T([
    F('customer_id', StringType()),
    F('first_name', StringType()),
    F('last_name', StringType()),
    F('phone', StringType()),
    F('email', StringType()),
    F('street', StringType()),
    F('city', StringType()),
    F('state', StringType()),
    F('zip_code', StringType()),
    F('updated_at', DateType()),
    F('is_active', StringType())
])

order_schema = T([
    F('order_id', StringType()),
    F('customer_id', StringType()),
    F('order_status', StringType()),
    F('order_date', DateType()),
    F('required_date', DateType()),
    F('shipped_date', DateType()),
    F('store_id', StringType()),
    F('staff_id', StringType()),
    F('product_id', StringType()),
    F('quantity', IntegerType()),
    F('list_price', DecimalType(10, 2)),
    F('discount', DecimalType(4, 2)),
    F('updated_at', DateType())
])

product_schema = T([
    F('product_id', StringType()),
    F('product_name', StringType()),
    F('brand_name', StringType()),
    F('category_name', StringType()),
    F('model_year', IntegerType()),
    F('list_price', DecimalType(10, 2)),
    F('updated_at', DateType()),
    F('is_active', StringType())
])

staff_schema = T([
    F('staff_id', StringType()),
    F('first_name', StringType()),
    F('last_name', StringType()),
    F('email', StringType()),
    F('phone', StringType()),
    F('manager_id', StringType()),
    F('updated_at', DateType()),
    F('is_active', StringType())
])

store_schema = T([
    F('store_id', StringType()),
    F('store_name', StringType()),
    F('phone', StringType()),
    F('email', StringType()),
    F('street', StringType()),
    F('city', StringType()),
    F('state', StringType()),
    F('zip_code', StringType()),
    F('updated_at', DateType()),
    F('is_active', StringType())
])


def load_data(spark: SparkSession, bucket_name: str, timestamp: str, table_name: str, schema: T) -> DataFrame:
    """
    Load extracted data from s3 raw bucket into spark.

    :param spark: Spark session.
    :param bucket_name: S3 raw bucket name from command line argument.
    :param timestamp: Date format as csv file name and path in s3 raw bucket from command line argument.
    :param table_name: Extracted csv table name.
    :param schema: Spark schema for each dataframe.
    """
    
    df = spark.read \
        .format('csv') \
        .options(
            header='true',
            path=f's3://{bucket_name}/{timestamp[0:4]}/{timestamp[4:6]}/{timestamp[6:8]}/{timestamp}-{table_name}.csv'
        ) \
        .schema(schema) \
        .load()
    
    return df


def create_stg_customers(df_customers: DataFrame) -> DataFrame:
    """
    Create staging customers table.
    
    :param df_customers: Extracted customer dataframe.
    """

    # rename columns
    stg_customers = df_customers.withColumnsRenamed({
        'updated_at': 'eff_date',
        'is_active': 'active_flag'
    })

    # drop null and duplicates
    stg_customers = stg_customers.dropna(subset=[
        'customer_id', 'first_name', 'last_name',
        'city', 'state', 'zip_code', 'eff_date', 'active_flag'
    ])
    stg_customers = stg_customers.dropDuplicates(subset=['customer_id', 'eff_date'])

    # rearrange columns
    stg_customers = stg_customers.select(
        'customer_id',
        'eff_date',
        'first_name',
        'last_name',
        'phone',
        'email',
        'street',
        'city',
        'state',
        'zip_code',
        'active_flag'
    ).orderBy(['eff_date', 'customer_id'])

    return stg_customers


def create_stg_stores(df_stores: DataFrame) -> DataFrame:
    """
    Create staging stores table.
    
    :param df_stores: Extracted store dataframe.
    """

    # rename columns
    stg_stores = df_stores.withColumnsRenamed({
        'updated_at': 'eff_date',
        'is_active': 'active_flag'
    })

    # drop null and duplicates
    stg_stores = stg_stores.dropna()
    stg_stores = stg_stores.dropDuplicates(subset=['store_id', 'eff_date'])

    # rearrange columns
    stg_stores = stg_stores.select(
        'store_id',
        'eff_date',
        'store_name',
        'phone',
        'email',
        'street',
        'city',
        'state',
        'zip_code',
        'active_flag'
    ).orderBy(['eff_date', 'store_id'])

    return stg_stores


def create_stg_staffs(df_staffs: DataFrame) -> DataFrame:
    """
    Create staging staffs table.
    
    :param df_staffs: Extracted staff dataframe.
    """

    # rename columns
    stg_staffs = df_staffs.withColumnsRenamed({
        'updated_at': 'eff_date',
        'is_active': 'active_flag'
    })

    # drop null and duplicates
    stg_staffs = stg_staffs.dropna()
    stg_staffs = stg_staffs.dropDuplicates(subset=['staff_id', 'eff_date'])

    # rearrange columns
    stg_staffs = stg_staffs.select(
        'staff_id',
        'eff_date',
        'first_name',
        'last_name',
        'email',
        'phone',
        'manager_id',
        'active_flag'
    ).orderBy(['eff_date', 'staff_id'])
    
    return stg_staffs


def create_stg_date(df_orders: DataFrame) -> DataFrame:
    """
    Create staging date table.
    
    :param df_orders: Extracted order dataframe.
    """

    # concat all date columns from orders dataframe then drop null and duplicates
    stg_date = df_orders.select('order_date').distinct() \
                        .union(df_orders.select('required_date').distinct()) \
                        .union(df_orders.select('shipped_date').distinct()) \
                        .union(df_orders.select('updated_at').distinct()) \
                        .dropna().dropDuplicates().orderBy('order_date')

    # rename column
    stg_date = stg_date.withColumnRenamed('order_date', 'date')

    # extract date components
    stg_date = stg_date.select(
        date_format('date', 'yyyyMMdd').alias('date_key'),
        'date',
        year('date').alias('year'),
        month('date').alias('month'),
        day('date').alias('day'),
        quarter('date').alias('quarter'),
        date_format('date', 'EEEE').alias('day_of_week'),
        date_format('date', 'MMMM').alias('month_name'),
        weekofyear('date').alias('week_of_year')
    )

    return stg_date


def create_stg_products(df_products: DataFrame) -> DataFrame:
    """
    Create staging products table.
    
    :param df_products: Extracted product dataframe.
    """

    # rename columns
    stg_products = df_products.withColumnsRenamed({
        'updated_at': 'eff_date',
        'is_active': 'active_flag'
    })

    # drop null and duplicates
    stg_products = stg_products.dropna()
    stg_products = stg_products.dropDuplicates(subset=['product_id', 'eff_date'])

    # remove double quote (") from product name
    stg_products = stg_products.withColumn('product_name', regexp_replace('product_name', '"', ''))

    # rearrange columns
    stg_products = stg_products.select(
        'product_id',
        'eff_date',
        'product_name',
        'brand_name',
        'category_name',
        'model_year',
        'list_price',
        'active_flag'
    ).orderBy(['eff_date', 'product_id'])

    return stg_products


def create_stg_sales(df_orders: DataFrame) -> DataFrame:
    """
    Create staging sales table.
    
    :param df_orders: Extracted order dataframe.
    """

    # drop null in all columns except shipped date
    stg_sales = df_orders.dropna(subset=[c for c in df_orders.columns if c != 'shipped_date'])
    
    # drop duplicates on primary key columns
    stg_sales = stg_sales.dropDuplicates(subset=['order_id', 'product_id'])
    
    # create measure columns
    stg_sales = stg_sales.withColumns({
        
        # display the date that order status updated to 'Reject'
        'rejected_date': when(col('order_status')=='Rejected', col('updated_at')).otherwise(None),
        
        # discount percentage of order
        'discount_percent': (col('discount') * 100).cast(IntegerType()),
        
        # total price of each order and product
        'total_price': (col('list_price') * col('quantity')).cast(DecimalType(15, 2)),
        
        # total discount amount
        'discount_amount': (col('total_price') * col('discount')).cast(DecimalType(15, 2)),
        
        # total price after calculated with discount
        'total_price_with_discount': (col('total_price') - col('discount_amount')).cast(DecimalType(15, 2))
        
    })
    
    # rearrange columns
    stg_sales = stg_sales.select(
        'order_id',
        'product_id',
        'customer_id',
        'store_id',
        'staff_id',
        date_format('order_date', 'yyyyMMdd').alias('order_date'),
        date_format('required_date', 'yyyyMMdd').alias('required_date'),
        date_format('shipped_date', 'yyyyMMdd').alias('shipped_date'),
        date_format('rejected_date', 'yyyyMMdd').alias('rejected_date'),
        'order_status',
        'quantity',
        'discount_percent',
        'discount_amount',
        'total_price',
        'total_price_with_discount'
    ).orderBy(['order_id', 'product_id'])
    
    return stg_sales


def write_output(df: DataFrame, bucket_name: str, file_path: str, output_dir: str) -> None:
    """
    Write output staging dataframes to s3 processed bucket.

    :param df: Dataframe to be saved.
    :param bucket_name: S3 processed bucket name.
    :param file_path: S3 bucket path.
    :param output_dir: Directory name to save file into.
    """

    df.coalesce(1).write \
        .format('parquet') \
        .mode('overwrite') \
        .options(
            maxRecordsPerFile=200000,
            path=f's3://{bucket_name}/{file_path}/{output_dir}/'
        ) \
        .save()
    

# main function
def main():

    # check for command line arguments
    if len(sys.argv) != 4:
        logging.error('Number of arguments is incorrect')
        sys.exit(1)

    # necessary variables
    raw_bucket_name = sys.argv[1]
    timestamp = sys.argv[2]
    processed_bucket_name = sys.argv[3]

    # format 'year/month/day'
    file_path = f'{timestamp[0:4]}/{timestamp[4:6]}/{timestamp[6:8]}'
    
    # create spark session
    logging.info('Creating spark session')
    spark = SparkSession \
        .builder \
        .appName('ETL_Transformation') \
        .getOrCreate()
    
    # transformation
    try:
        logging.info('Loading raw data from s3')
        df_customers = load_data(spark, raw_bucket_name, timestamp, 'customers', customer_schema)
        df_orders = load_data(spark, raw_bucket_name, timestamp, 'orders', order_schema)
        df_products = load_data(spark, raw_bucket_name, timestamp, 'products', product_schema)
        df_staffs = load_data(spark, raw_bucket_name, timestamp, 'staffs', staff_schema)
        df_stores = load_data(spark, raw_bucket_name, timestamp, 'stores', store_schema)
        
        logging.info('Transforming tables')
        stg_customers = create_stg_customers(df_customers)
        stg_stores = create_stg_stores(df_stores)
        stg_staffs = create_stg_staffs(df_staffs)
        stg_date = create_stg_date(df_orders)
        stg_products = create_stg_products(df_products)
        stg_sales = create_stg_sales(df_orders)
        
        logging.info('Saving output to s3')
        write_output(stg_customers, processed_bucket_name, file_path, 'customers')
        write_output(stg_stores, processed_bucket_name, file_path, 'stores')
        write_output(stg_staffs, processed_bucket_name, file_path, 'staffs')
        write_output(stg_date, processed_bucket_name, file_path, 'date')
        write_output(stg_products, processed_bucket_name, file_path, 'products')
        write_output(stg_sales, processed_bucket_name, file_path, 'sales')

    # log error
    except Exception as e:
        logging.error(f'Error occurred: {e}')
        sys.exit(1)

    # shutdown the session
    finally:
        spark.stop()


# run main function
if __name__ == '__main__':
    main()