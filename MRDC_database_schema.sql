-- Cast columns in orders_table to correct data type
ALTER TABLE orders_table
 ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
 ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
 ALTER COLUMN card_number TYPE VARCHAR(19),
 ALTER COLUMN store_code TYPE VARCHAR(12),
 ALTER COLUMN product_code TYPE VARCHAR(11),
 ALTER COLUMN product_quantity TYPE SMALLINT;

-- Cast columns in dim_users table to correct data type
ALTER TABLE dim_users
 ALTER COLUMN first_name TYPE VARCHAR(255),
 ALTER COLUMN last_name TYPE VARCHAR(255),
 ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
 ALTER COLUMN country_code TYPE VARCHAR(2),
 ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
 ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

-- Alter latitude and longitude values so that the column can be converted to float data type
UPDATE dim_store_details
SET latitude = 0.0
WHERE latitude = 'N/A';

UPDATE dim_store_details
SET longitude = 0.0
WHERE longitude = NULL;

-- Alter columns in dim_store_details to correct data type
ALTER TABLE dim_store_details
 ALTER COLUMN locality TYPE VARCHAR(255),
 ALTER COLUMN store_code TYPE VARCHAR(12),
 ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
 ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
 ALTER COLUMN store_type TYPE VARCHAR(255),
 ALTER COLUMN country_code TYPE VARCHAR(2),
 ALTER COLUMN continent TYPE VARCHAR(255),
 ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
 ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision;

-- Remove '£' symbol from product_price column in dim_product table
UPDATE dim_product
SET product_price = CAST(REPLACE(CAST(product_price AS text), '£', '') AS double precision);

-- Create another column to help diffrentiate the weights
ALTER TABLE dim_product
ADD COLUMN weight_class VARCHAR(14);

UPDATE dim_product
SET weight_class =
 CASE
  WHEN weight < 2 THEN 'Light'
  WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
  WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
  WHEN weight >= 140 THEN 'Truck_Required'
 END;

-- Change values in removed column to true or false in dim_product
UPDATE dim_product
SET removed = 
 CASE 
  WHEN removed = 'Still_avaliable' THEN true
  WHEN removed = 'Removed' THEN false
 END;

-- Rename removed column to Still_avaliable in dim_product
ALTER TABLE dim_product
RENAME COLUMN removed TO still_available;

-- Alter columns in dim_product table to correct data type
ALTER TABLE dim_product
 ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
 ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
 ALTER COLUMN "EAN" TYPE VARCHAR(17),
 ALTER COLUMN product_code TYPE VARCHAR(11),
 ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
 ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
 ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN;

-- Alter columns in dim_date_times table to correct data types
ALTER TABLE dim_date_times
 ALTER COLUMN "month" TYPE VARCHAR(2),
 ALTER COLUMN "year" TYPE VARCHAR(4),
 ALTER COLUMN "day" TYPE VARCHAR(2),
 ALTER COLUMN time_period TYPE VARCHAR(10),
 ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- Alter columns in dim_card_details to correct data types
ALTER TABLE dim_card_details
 ALTER COLUMN card_number TYPE VARCHAR(19),
 ALTER COLUMN expiry_date TYPE VARCHAR (19),
 ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

-- Create a primary key for card_number column in dim_card_details table
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

-- Create a primary key for date_uuid column in dim_date_times table
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

-- Create a primary key for store_code column in dim_store_details
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

-- Create a primary key for product_code column in dim_product table
ALTER TABLE dim_product
ADD PRIMARY KEY (product_code);

-- Create a primary key for user_uuid column in dim_users table
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

-- Foreign key constraint for card_number
ALTER TABLE orders_table ADD CONSTRAINT fk_orders_card_details
FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

-- Foreign key constraint for date_uuid
ALTER TABLE orders_table ADD CONSTRAINT fk_orders_date_times
FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

-- Foreign key constraint for store_code
ALTER TABLE orders_table ADD CONSTRAINT fk_orders_store_details
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

-- Foreign key constraint for product_code
ALTER TABLE orders_table ADD CONSTRAINT fk_orders_product
FOREIGN KEY (product_code) REFERENCES dim_product(product_code);

-- Foreign key constraint for user_uuid
ALTER TABLE orders_table ADD CONSTRAINT fk_orders_users
FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);