-- create trigger function for insert and update operation
CREATE OR REPLACE FUNCTION store.trg_update_date()
RETURNS TRIGGER
AS $$
BEGIN

    IF TG_OP = 'INSERT' THEN
        NEW.created_at := CURRENT_DATE;
        NEW.is_active := 'Y';

    END IF;

    NEW.updated_at := CURRENT_DATE;

    RETURN NEW;

END;
$$ LANGUAGE plpgsql;

-- trigger for each table

CREATE OR REPLACE TRIGGER trg_customer
BEFORE INSERT OR UPDATE ON store.customers
FOR EACH ROW
EXECUTE PROCEDURE store.trg_update_date();

CREATE OR REPLACE TRIGGER trg_staff
BEFORE INSERT OR UPDATE ON store.staffs
FOR EACH ROW
EXECUTE PROCEDURE store.trg_update_date();

CREATE OR REPLACE TRIGGER trg_product
BEFORE INSERT OR UPDATE ON store.products
FOR EACH ROW
EXECUTE PROCEDURE store.trg_update_date();

CREATE OR REPLACE TRIGGER trg_store
BEFORE INSERT OR UPDATE ON store.stores
FOR EACH ROW
EXECUTE PROCEDURE store.trg_update_date();


-- trigger function for order table
CREATE OR REPLACE FUNCTION store.trg_order()
RETURNS TRIGGER
AS $$
BEGIN

    NEW.updated_at = CURRENT_DATE;

    RETURN NEW;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_order
BEFORE INSERT OR UPDATE ON store.orders
FOR EACH ROW
EXECUTE PROCEDURE store.trg_order();


-- soft deletion rule

CREATE OR REPLACE RULE rule_customer_soft_delete AS
ON DELETE TO store.customers
DO INSTEAD (
    UPDATE store.customers
    SET is_active = 'N'
    WHERE customer_id = OLD.customer_id
);

CREATE OR REPLACE RULE rule_staff_soft_delete AS
ON DELETE TO store.staffs
DO INSTEAD (
    UPDATE store.staffs
    SET is_active = 'N'
    WHERE staff_id = OLD.staff_id
);

CREATE OR REPLACE RULE rule_product_soft_delete AS
ON DELETE TO store.products
DO INSTEAD (
    UPDATE store.products
    SET is_active = 'N'
    WHERE product_id = OLD.product_id
);

CREATE OR REPLACE RULE rule_store_soft_delete AS
ON DELETE TO store.stores
DO INSTEAD (
    UPDATE store.stores
    SET is_active = 'N'
    WHERE store_id = OLD.store_id
);