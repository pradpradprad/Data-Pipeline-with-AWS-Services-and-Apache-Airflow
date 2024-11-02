-- create new schema if not exists
CREATE SCHEMA IF NOT EXISTS store;


-- drop tables with all their dependent objects
DROP TABLE IF EXISTS store.stores CASCADE;
DROP TABLE IF EXISTS store.staffs CASCADE;
DROP TABLE IF EXISTS store.categories CASCADE;
DROP TABLE IF EXISTS store.brands CASCADE;
DROP TABLE IF EXISTS store.products CASCADE;
DROP TABLE IF EXISTS store.customers CASCADE;
DROP TABLE IF EXISTS store.order_status CASCADE;
DROP TABLE IF EXISTS store.orders CASCADE;
DROP TABLE IF EXISTS store.order_items CASCADE;
DROP TABLE IF EXISTS store.stocks CASCADE;


-- create s3 extension
CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;


-- create main tables

CREATE TABLE store.stores (
	store_id    VARCHAR(15) PRIMARY KEY,
	store_name  VARCHAR(255) NOT NULL,
	phone       VARCHAR(25),
	email       VARCHAR(255),
	street      VARCHAR(255),
	city        VARCHAR(255),
	state       VARCHAR(10),
	zip_code    VARCHAR(5),
    created_at  DATE,
    updated_at  DATE,
    is_active   VARCHAR(1)
);

CREATE TABLE store.staffs (
	staff_id                    VARCHAR(15) PRIMARY KEY,
	first_name                  VARCHAR(50) NOT NULL,
	last_name                   VARCHAR(50) NOT NULL,
	email                       VARCHAR(255) NOT NULL UNIQUE,
	phone                       VARCHAR(25),
	store_id                    VARCHAR(15) NOT NULL,
	manager_id                  VARCHAR(15),
    created_at                  DATE,
    updated_at                  DATE,
    is_active                   VARCHAR(1),
	FOREIGN KEY (store_id)      REFERENCES store.stores (store_id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (manager_id)    REFERENCES store.staffs (staff_id) 
        ON DELETE NO ACTION
        ON UPDATE NO ACTION
);

CREATE TABLE store.categories (
	category_id     INT PRIMARY KEY,
	category_name   VARCHAR(255) NOT NULL
);

CREATE TABLE store.brands (
	brand_id    INT PRIMARY KEY,
	brand_name  VARCHAR(255) NOT NULL
);

CREATE TABLE store.products (
	product_id                  VARCHAR(15) PRIMARY KEY,
	product_name                VARCHAR(255) NOT NULL,
	brand_id                    INT NOT NULL,
	category_id                 INT NOT NULL,
	model_year                  INT NOT NULL,
	list_price                  DECIMAL(10, 2) NOT NULL,
    created_at                  DATE,
    updated_at                  DATE,
    is_active                   VARCHAR(1),
	FOREIGN KEY (category_id)   REFERENCES store.categories (category_id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (brand_id)      REFERENCES store.brands (brand_id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE store.customers (
	customer_id VARCHAR(15) PRIMARY KEY,
	first_name  VARCHAR(255) NOT NULL,
	last_name   VARCHAR(255) NOT NULL,
	phone       VARCHAR(25),
	email       VARCHAR(255) NOT NULL,
	street      VARCHAR(255),
	city        VARCHAR(50),
	state       VARCHAR(25),
	zip_code    VARCHAR(5),
    created_at  DATE,
    updated_at  DATE,
    is_active   VARCHAR(1)
);

CREATE TABLE store.order_status (
    order_status_id INT PRIMARY KEY,
    order_status    VARCHAR(15) NOT NULL
);

CREATE TABLE store.orders (
	order_id                        VARCHAR(15) PRIMARY KEY,
	customer_id                     VARCHAR(15) NOT NULL,
	order_status_id                 INT NOT NULL,
	order_date                      DATE NOT NULL,
	required_date                   DATE NOT NULL,
	shipped_date                    DATE,
	store_id                        VARCHAR(15) NOT NULL,
	staff_id                        VARCHAR(15) NOT NULL,
    updated_at                      DATE,
	FOREIGN KEY (customer_id)       REFERENCES store.customers (customer_id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (order_status_id)   REFERENCES store.order_status (order_status_id)
        ON DELETE NO ACTION
        ON UPDATE CASCADE,
	FOREIGN KEY (store_id)          REFERENCES store.stores (store_id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (staff_id)          REFERENCES store.staffs (staff_id) 
        ON DELETE NO ACTION
        ON UPDATE NO ACTION
);

CREATE TABLE store.order_items (
	order_id                    VARCHAR(15),
	item_id                     INT,
	product_id                  VARCHAR(15) NOT NULL,
	quantity                    INT NOT NULL,
	list_price                  DECIMAL(10, 2) NOT NULL,
	discount                    DECIMAL(4, 2) NOT NULL DEFAULT 0,
	PRIMARY KEY (order_id, item_id),
	FOREIGN KEY (order_id)      REFERENCES store.orders (order_id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (product_id)    REFERENCES store.products (product_id) 
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE store.stocks (
	store_id                    VARCHAR(15),
	product_id                  VARCHAR(15),
	quantity                    INT,
	PRIMARY KEY (store_id, product_id),
	FOREIGN KEY (store_id)      REFERENCES store.stores (store_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (product_id)    REFERENCES store.products (product_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);