-- How many stores and in which country
SELECT 
 country_code, 
 COUNT (*) AS total_no_stores
FROM 
 dim_store_details
GROUP BY 
 country_code
ORDER BY 
 total_no_stores DESC;

-- Which location has the most stores
SELECT 
 locality, 
 COUNT (*) AS total_no_stores
FROM 
 dim_store_details
GROUP BY 
 locality
ORDER BY 
 total_no_stores DESC;

-- Which months have largest amount of sales
SELECT 
 SUM(product_price * orders_table.product_quantity) AS total_sales, 
 dim_date_times.month
FROM 
 dim_product
JOIN 
 orders_table ON dim_product.product_code = orders_table.product_code
JOIN 
 dim_date_times on orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY 
 dim_date_times.month
ORDER BY 
 total_sales DESC;

-- How many sales are coming from online
SELECT 
 COUNT(*) AS number_of_sales, 
 SUM(orders_table.product_quantity) AS product_quantity_count,
 CASE
  WHEN 
   orders_table.store_code = 'WEB-1388012W' THEN 'Web' ELSE 'Offline'
 END 
  AS "location"
FROM 
 orders_table
JOIN 
 dim_card_details ON orders_table.card_number = dim_card_details.card_number
GROUP BY 
 "location";
 
-- Percentage of sales through each store
SELECT
 dim_store_details.store_type,
 SUM(dim_product.product_price * orders_table.product_quantity) AS total_sales,
 (SUM(dim_product.product_price * orders_table.product_quantity) / SUM(SUM(dim_product.product_price * orders_table.product_quantity)) OVER ()) * 100 AS percentage_total
FROM
  orders_table
JOIN
  dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN
  dim_product ON orders_table.product_code = dim_product.product_code
GROUP BY
  dim_store_details.store_type
ORDER BY
  total_sales DESC;

-- Which month in each year has highest cost of sales
SELECT
 SUM(p.product_price * o.product_quantity) AS total_sales,
 d.year AS year,
 d.month AS month
FROM
 orders_table o
JOIN
 dim_date_times d ON o.date_uuid = d.date_uuid
JOIN
 dim_product p ON o.product_code = p.product_code
GROUP BY
 year, month
ORDER BY
 total_sales DESC;

-- Staff headcount in each country
SELECT 
 SUM(staff_numbers) AS total_staff_numbers, 
 country_code
FROM 
 dim_store_details
GROUP BY 
 country_code
ORDER BY 
 total_staff_numbers DESC;

-- Which German store type is selling the most
SELECT 
 SUM(dim_product.product_price * orders_table.product_quantity) AS total_sales, 
 dim_store_details.store_type, 
 dim_store_details.country_code
FROM 
 dim_store_details
JOIN 
 orders_table ON dim_store_details.store_code = orders_table.store_code
JOIN 
 dim_product ON orders_table.product_code = dim_product.product_code
WHERE 
 dim_store_details.country_code = 'DE'
GROUP BY 
 dim_store_details.store_type, 
 dim_store_details.country_code
ORDER BY 
 total_sales ASC;

-- Average time taken between sales each year
WITH combined_datetime AS (
 SELECT
  day,
  month,
  year,
  timestamp,
  (year || '-' || month || '-' || day || ' ' || timestamp)::TIMESTAMP AS full_datetime
 FROM
   dim_date_times
),
 lead_datetime AS (
  SELECT
   day,
   month,
   year,
   timestamp,
   full_datetime,
   LEAD(full_datetime) OVER (PARTITION BY year ORDER BY full_datetime) AS next_datetime
  FROM
   combined_datetime
),
 time_difference AS (
  SELECT
   day,
   month,
   year,
   timestamp,
   full_datetime,
   next_datetime,
   next_datetime - full_datetime AS time_taken
  FROM
   lead_datetime
)
SELECT
 year,
  CONCAT(
   '"hours": ', EXTRACT(HOUR FROM AVG(time_taken))::INTEGER, ', ',
   '"minutes": ', EXTRACT(MINUTE FROM AVG(time_taken))::INTEGER, ', ',
   '"seconds": ', EXTRACT(SECOND FROM AVG(time_taken))::INTEGER, ', ',
   '"milliseconds": ', EXTRACT(MILLISECOND FROM AVG(time_taken))::INTEGER
) AS actual_time_taken
FROM
 time_difference
GROUP BY
 year
ORDER BY
 year;