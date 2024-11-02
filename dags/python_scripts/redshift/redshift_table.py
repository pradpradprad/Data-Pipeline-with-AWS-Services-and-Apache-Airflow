# create schema
create_staging = 'CREATE SCHEMA IF NOT EXISTS staging;'
create_serving = 'CREATE SCHEMA IF NOT EXISTS serving;'


# create main tables

create_dim_customers = '''
CREATE TABLE IF NOT EXISTS serving.dim_customers (
    customer_key    INT IDENTITY(1, 1),
    customer_id     VARCHAR(15) NOT NULL,
    first_name      VARCHAR(255) NOT NULL,
    last_name       VARCHAR(255) NOT NULL,
    phone           VARCHAR(15),
    email           VARCHAR(255),
    street          VARCHAR(255),
    city            VARCHAR(255) NOT NULL,
    state           VARCHAR(2) NOT NULL,
    zip_code        VARCHAR(5) NOT NULL,
    eff_date        DATE NOT NULL,
    exp_date        DATE NOT NULL,
    active_flag     VARCHAR(1) NOT NULL,
    PRIMARY KEY (customer_key)
);
'''

create_dim_stores = '''
CREATE TABLE IF NOT EXISTS serving.dim_stores (
    store_key   INT IDENTITY(1, 1),
    store_id    VARCHAR(15) NOT NULL,
    store_name  VARCHAR(255) NOT NULL,
    phone       VARCHAR(15) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    street      VARCHAR(255) NOT NULL,
    city        VARCHAR(255) NOT NULL,
    state       VARCHAR(2) NOT NULL,
    zip_code    VARCHAR(5) NOT NULL,
    eff_date    DATE NOT NULL,
    exp_date    DATE NOT NULL,
    active_flag VARCHAR(1) NOT NULL,
    PRIMARY KEY (store_key)
);
'''

create_dim_staffs = '''
CREATE TABLE IF NOT EXISTS serving.dim_staffs (
    staff_key   INT IDENTITY(1, 1),
    staff_id    VARCHAR(15) NOT NULL,
    first_name  VARCHAR(255) NOT NULL,
    last_name   VARCHAR(255) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    phone       VARCHAR(15) NOT NULL,
    manager_id  VARCHAR(15) NOT NULL,
    eff_date    DATE NOT NULL,
    exp_date    DATE NOT NULL,
    active_flag VARCHAR(1) NOT NULL,
    PRIMARY KEY (staff_key)
);
'''

create_dim_date = '''
CREATE TABLE IF NOT EXISTS serving.dim_date (
    date_key        VARCHAR(8),
    date            DATE NOT NULL,
    year            INT NOT NULL,
    month           INT NOT NULL,
    day             INT NOT NULL,
    quarter         INT NOT NULL,
    day_of_week     VARCHAR(9) NOT NULL,
    month_name      VARCHAR(9) NOT NULL,
    week_of_year    INT NOT NULL,
    PRIMARY KEY (date_key)
);
'''

create_dim_products = '''
CREATE TABLE IF NOT EXISTS serving.dim_products (
    product_key     INT IDENTITY(1, 1),
    product_id      VARCHAR(15) NOT NULL,
    product_name    VARCHAR(255) NOT NULL,
    brand_name      VARCHAR(255) NOT NULL,
    category_name   VARCHAR(255) NOT NULL,
    model_year      INT NOT NULL,
    list_price      NUMERIC(10, 2) NOT NULL,
    eff_date        DATE NOT NULL,
    exp_date        DATE NOT NULL,
    active_flag     VARCHAR(1) NOT NULL,
    PRIMARY KEY (product_key)
);
'''

create_dim_order_status = '''
CREATE TABLE IF NOT EXISTS serving.dim_order_status (
    order_status_id INT,
    order_status    VARCHAR(15) NOT NULL,
    PRIMARY KEY (order_status_id)
);
'''

create_fact_sales = '''
CREATE TABLE IF NOT EXISTS serving.fact_sales (
    sales_id                    INT IDENTITY(1, 1),
    order_id                    VARCHAR(15) NOT NULL,
    product_key                 INT NOT NULL,
    customer_key                INT NOT NULL,
    store_key                   INT NOT NULL,
    staff_key                   INT NOT NULL,
    order_date                  VARCHAR(8) NOT NULL,
    required_date               VARCHAR(8) NOT NULL,
    shipped_date                VARCHAR(8),
    rejected_date               VARCHAR(8),
    order_status_id             INT NOT NULL,
    quantity                    INT NOT NULL,
    discount_percent            INT NOT NULL,
    discount_amount             NUMERIC(15, 2) NOT NULL,
    total_price                 NUMERIC(15, 2) NOT NULL,
    total_price_with_discount   NUMERIC(15, 2) NOT NULL,
    PRIMARY KEY (sales_id)
);
'''


# create staging tables

create_stg_customers = '''
CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id VARCHAR(15),
    eff_date    DATE,
    first_name  VARCHAR(255) NOT NULL,
    last_name   VARCHAR(255) NOT NULL,
    phone       VARCHAR(15),
    email       VARCHAR(255),
    street      VARCHAR(255),
    city        VARCHAR(255) NOT NULL,
    state       VARCHAR(2) NOT NULL,
    zip_code    VARCHAR(5) NOT NULL,
    active_flag VARCHAR(1) NOT NULL,
    PRIMARY KEY (customer_id, eff_date)
);
'''

create_stg_stores = '''
CREATE TABLE IF NOT EXISTS staging.stg_stores (
    store_id    VARCHAR(15),
    eff_date    DATE,
    store_name  VARCHAR(255) NOT NULL,
    phone       VARCHAR(15) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    street      VARCHAR(255) NOT NULL,
    city        VARCHAR(255) NOT NULL,
    state       VARCHAR(2) NOT NULL,
    zip_code    VARCHAR(5) NOT NULL,
    active_flag VARCHAR(1) NOT NULL,
    PRIMARY KEY (store_id, eff_date)
);
'''

create_stg_staffs = '''
CREATE TABLE IF NOT EXISTS staging.stg_staffs (
    staff_id    VARCHAR(15),
    eff_date    DATE,
    first_name  VARCHAR(255) NOT NULL,
    last_name   VARCHAR(255) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    phone       VARCHAR(15) NOT NULL,
    manager_id  VARCHAR(15) NOT NULL,
    active_flag VARCHAR(1) NOT NULL,
    PRIMARY KEY (staff_id, eff_date)
);
'''

create_stg_date = '''
CREATE TABLE IF NOT EXISTS staging.stg_date (
    date_key        VARCHAR(8),
    date            DATE NOT NULL,
    year            INT NOT NULL,
    month           INT NOT NULL,
    day             INT NOT NULL,
    quarter         INT NOT NULL,
    day_of_week     VARCHAR(9) NOT NULL,
    month_name      VARCHAR(9) NOT NULL,
    week_of_year    INT NOT NULL,
    PRIMARY KEY (date_key)
);
'''

create_stg_products = '''
CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id      VARCHAR(15),
    eff_date        DATE,
    product_name    VARCHAR(255) NOT NULL,
    brand_name      VARCHAR(255) NOT NULL,
    category_name   VARCHAR(255) NOT NULL,
    model_year      INT NOT NULL,
    list_price      NUMERIC(10, 2) NOT NULL,
    active_flag     VARCHAR(1) NOT NULL,
    PRIMARY KEY (product_id, eff_date)
);
'''

create_stg_sales = '''
CREATE TABLE IF NOT EXISTS staging.stg_sales (
    order_id                    VARCHAR(15),
    product_id                  VARCHAR(15),
    customer_id                 VARCHAR(15) NOT NULL,
    store_id                    VARCHAR(15) NOT NULL,
    staff_id                    VARCHAR(15) NOT NULL,
    order_date                  VARCHAR(8) NOT NULL,
    required_date               VARCHAR(8) NOT NULL,
    shipped_date                VARCHAR(8),
    rejected_date               VARCHAR(8),
    order_status                VARCHAR(15) NOT NULL,
    quantity                    INT NOT NULL,
    discount_percent            INT NOT NULL,
    discount_amount             NUMERIC(15, 2) NOT NULL,
    total_price                 NUMERIC(15, 2) NOT NULL,
    total_price_with_discount   NUMERIC(15, 2) NOT NULL,
    PRIMARY KEY (order_id, product_id)
);
'''


# insert order status records
insert_dim_order_status = '''
INSERT INTO serving.dim_order_status (order_status_id, order_status)
VALUES
    (1, 'Pending'),
    (2, 'Rejected'),
    (3, 'Completed');
'''


setup_tables = [

    create_staging, create_serving,

    create_dim_customers, create_dim_stores, create_dim_staffs,
    create_dim_date, create_dim_products, create_dim_order_status, create_fact_sales,

    create_stg_customers, create_stg_stores, create_stg_staffs,
    create_stg_date, create_stg_products, create_stg_sales,

    insert_dim_order_status

]