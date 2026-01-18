CREATE SCHEMA IF NOT EXISTS silver;

-- -------------------------
-- 1) HOTELS (dedupe + basic checks)
-- -------------------------
DROP TABLE IF EXISTS silver.hotels;
CREATE TABLE silver.hotels AS
WITH base AS (
  SELECT
    trim(hotel_id) AS hotel_id,
    NULLIF(trim(hotel_name),'') AS hotel_name,
    initcap(NULLIF(trim(city),'')) AS city,
    NULLIF(total_rooms::int, NULL) AS total_rooms,
    row_number() OVER (PARTITION BY trim(hotel_id) ORDER BY total_rooms DESC NULLS LAST) AS rn
  FROM bronze.hotels_raw
),
valid AS (
  SELECT * FROM base
  WHERE rn = 1
    AND hotel_id IS NOT NULL AND hotel_id <> ''
    AND city IN ('Hyderabad','Bengaluru','Delhi','Mumbai','Chennai')
    AND total_rooms BETWEEN 10 AND 500
)
SELECT hotel_id, hotel_name, city, total_rooms
FROM valid;

-- rejected: invalid hotels (PK missing / bad city / bad rooms)
INSERT INTO audit.rejected_rows(run_id, layer, source_table, target_table, reason, row_data)
SELECT :'run_id', 'silver', 'bronze.hotels_raw', 'silver.hotels',
       'INVALID_HOTEL_FIELDS',
       to_jsonb(r)
FROM bronze.hotels_raw r
WHERE r.hotel_id IS NULL OR trim(r.hotel_id) = ''
   OR initcap(trim(r.city)) NOT IN ('Hyderabad','Bengaluru','Delhi','Mumbai','Chennai')
   OR r.total_rooms::int NOT BETWEEN 10 AND 500;

-- -------------------------
-- 2) ROOM TYPES (dedupe + FK to hotels + enum check)
-- -------------------------
DROP TABLE IF EXISTS silver.room_types;
CREATE TABLE silver.room_types AS
WITH base AS (
  SELECT
    trim(room_type_id) AS room_type_id,
    trim(hotel_id) AS hotel_id,
    initcap(NULLIF(trim(room_type),'')) AS room_type,
    base_rate::numeric AS base_rate,
    row_number() OVER (PARTITION BY trim(room_type_id) ORDER BY base_rate DESC NULLS LAST) AS rn
  FROM bronze.room_types_raw
),
valid AS (
  SELECT b.*
  FROM base b
  JOIN silver.hotels h ON h.hotel_id = b.hotel_id
  WHERE b.rn = 1
    AND b.room_type_id IS NOT NULL AND b.room_type_id <> ''
    AND b.room_type IN ('Standard','Deluxe','Suite','Family')
    AND b.base_rate BETWEEN 1000 AND 50000
)
SELECT room_type_id, hotel_id, room_type, base_rate
FROM valid;

-- rejected: FK missing hotel OR invalid fields
INSERT INTO audit.rejected_rows(run_id, layer, source_table, target_table, reason, row_data)
SELECT :'run_id', 'silver', 'bronze.room_types_raw', 'silver.room_types',
       'INVALID_ROOMTYPE_OR_FK',
       to_jsonb(r)
FROM bronze.room_types_raw r
LEFT JOIN silver.hotels h ON h.hotel_id = trim(r.hotel_id)
WHERE h.hotel_id IS NULL
   OR r.room_type_id IS NULL OR trim(r.room_type_id) = ''
   OR initcap(trim(r.room_type)) NOT IN ('Standard','Deluxe','Suite','Family')
   OR r.base_rate::numeric NOT BETWEEN 1000 AND 50000;

-- -------------------------
-- 3) BOOKINGS (type enforcement, enum normalization, date checks)
-- -------------------------
DROP TABLE IF EXISTS silver.bookings;
CREATE TABLE silver.bookings AS
WITH base AS (
  SELECT
    trim(booking_id) AS booking_id,
    trim(hotel_id) AS hotel_id,
    initcap(NULLIF(trim(city),'')) AS city,
    initcap(NULLIF(trim(room_type),'')) AS room_type,

    booking_date::date AS booking_date,
    checkin_date::date AS checkin_date,
    checkout_date::date AS checkout_date,
    nights::int AS nights,

    upper(trim(booking_channel)) AS booking_channel,
    initcap(trim(market_segment)) AS market_segment,
    initcap(trim(booking_status)) AS booking_status,

    room_price_per_night::numeric AS room_price_per_night,
    total_amount::numeric AS total_amount,

    row_number() OVER (PARTITION BY trim(booking_id) ORDER BY booking_date DESC NULLS LAST) AS rn
  FROM bronze.bookings_raw
),
normalized AS (
  SELECT
    booking_id,
    hotel_id,
    city,
    room_type,
    booking_date,
    checkin_date,
    checkout_date,
    CASE
      WHEN nights IS NULL OR nights <= 0 THEN (checkout_date::date - checkin_date::date)
      ELSE nights
    END AS nights,

    CASE
      WHEN booking_channel IN ('OTA','ONLINE TRAVEL AGENTS') THEN 'OTA'
      WHEN booking_channel IN ('DIRECT') THEN 'Direct'
      WHEN booking_channel IN ('CORPORATE') THEN 'Corporate'
      ELSE 'Other'
    END AS booking_channel,

    CASE
      WHEN market_segment IN ('Leisure','Corporate','Group') THEN market_segment
      ELSE 'Leisure'
    END AS market_segment,

    CASE
      WHEN booking_status IN ('Confirmed','Cancelled') THEN booking_status
      ELSE 'Confirmed'
    END AS booking_status,

    room_price_per_night,
    total_amount,
    rn
  FROM base
),
valid AS (
  SELECT n.*
  FROM normalized n
  JOIN silver.hotels h ON h.hotel_id = n.hotel_id
  WHERE n.rn = 1
    AND n.booking_id IS NOT NULL AND n.booking_id <> ''
    AND n.city IN ('Hyderabad','Bengaluru','Delhi','Mumbai','Chennai')
    AND n.room_type IN ('Standard','Deluxe','Suite','Family')
    AND n.booking_channel IN ('OTA','Direct','Corporate','Other')
    AND n.booking_status IN ('Confirmed','Cancelled')
    AND n.market_segment IN ('Leisure','Corporate','Group')
    AND n.checkin_date BETWEEN DATE '2025-07-01' AND DATE '2025-12-31'
    AND n.checkout_date >= n.checkin_date
    AND n.nights BETWEEN 1 AND 30
    AND n.room_price_per_night BETWEEN 500 AND 100000
    AND n.total_amount >= 0
)
SELECT
  booking_id, hotel_id, city, room_type,
  booking_date, checkin_date, checkout_date, nights,
  booking_channel, market_segment, booking_status,
  room_price_per_night, total_amount
FROM valid;

-- rejected: invalid booking fields or missing FK hotel
INSERT INTO audit.rejected_rows(run_id, layer, source_table, target_table, reason, row_data)
SELECT :'run_id', 'silver', 'bronze.bookings_raw', 'silver.bookings',
       'INVALID_BOOKING_OR_FK',
       to_jsonb(r)
FROM bronze.bookings_raw r
LEFT JOIN silver.hotels h ON h.hotel_id = trim(r.hotel_id)
WHERE h.hotel_id IS NULL
   OR r.booking_id IS NULL OR trim(r.booking_id) = ''
   OR initcap(trim(r.city)) NOT IN ('Hyderabad','Bengaluru','Delhi','Mumbai','Chennai')
   OR initcap(trim(r.room_type)) NOT IN ('Standard','Deluxe','Suite','Family')
   OR initcap(trim(r.booking_status)) NOT IN ('Confirmed','Cancelled')
   OR r.checkin_date::date < DATE '2025-07-01'
   OR r.checkin_date::date > DATE '2025-12-31'
   OR r.checkout_date::date < r.checkin_date::date
   OR r.room_price_per_night::numeric < 500
   OR r.total_amount::numeric < 0;

-- -------------------------
-- 4) PAYMENTS (FK to bookings + numeric checks)
-- -------------------------
DROP TABLE IF EXISTS silver.payments;
CREATE TABLE silver.payments AS
WITH base AS (
  SELECT
    trim(payment_id) AS payment_id,
    trim(booking_id) AS booking_id,
    initcap(trim(payment_status)) AS payment_status,
    paid_amount::numeric AS paid_amount,
    payment_date::date AS payment_date,
    row_number() OVER (PARTITION BY trim(payment_id) ORDER BY payment_date DESC NULLS LAST) AS rn
  FROM bronze.payments_raw
),
valid AS (
  SELECT b.*
  FROM base b
  JOIN silver.bookings bk ON bk.booking_id = b.booking_id
  WHERE b.rn = 1
    AND b.payment_id IS NOT NULL AND b.payment_id <> ''
    AND b.payment_status IN ('Paid','Failed','Refunded')
    AND b.paid_amount >= 0
)
SELECT payment_id, booking_id, payment_status, paid_amount, payment_date
FROM valid;

INSERT INTO audit.rejected_rows(run_id, layer, source_table, target_table, reason, row_data)
SELECT :'run_id', 'silver', 'bronze.payments_raw', 'silver.payments',
       'INVALID_PAYMENT_OR_FK',
       to_jsonb(r)
FROM bronze.payments_raw r
LEFT JOIN silver.bookings b ON b.booking_id = trim(r.booking_id)
WHERE b.booking_id IS NULL
   OR r.payment_id IS NULL OR trim(r.payment_id) = ''
   OR initcap(trim(r.payment_status)) NOT IN ('Paid','Failed','Refunded')
   OR r.paid_amount::numeric < 0;

-- -------------------------
-- 5) ROOM INVENTORY (FK to hotels + range checks)
-- -------------------------
DROP TABLE IF EXISTS silver.room_inventory;
CREATE TABLE silver.room_inventory AS
WITH base AS (
  SELECT
    trim(hotel_id) AS hotel_id,
    date::date AS date,
    available_rooms::int AS available_rooms,
    occupied_rooms::int AS occupied_rooms
  FROM bronze.room_inventory_raw
),
valid AS (
  SELECT b.*
  FROM base b
  JOIN silver.hotels h ON h.hotel_id = b.hotel_id
  WHERE b.date BETWEEN DATE '2025-07-01' AND DATE '2025-12-31'
    AND b.available_rooms BETWEEN 1 AND 1000
    AND b.occupied_rooms BETWEEN 0 AND b.available_rooms
)
SELECT hotel_id, date, available_rooms, occupied_rooms
FROM valid;

INSERT INTO audit.rejected_rows(run_id, layer, source_table, target_table, reason, row_data)
SELECT :'run_id', 'silver', 'bronze.room_inventory_raw', 'silver.room_inventory',
       'INVALID_INVENTORY_OR_FK',
       to_jsonb(r)
FROM bronze.room_inventory_raw r
LEFT JOIN silver.hotels h ON h.hotel_id = trim(r.hotel_id)
WHERE h.hotel_id IS NULL
   OR r.date::date < DATE '2025-07-01' OR r.date::date > DATE '2025-12-31'
   OR r.available_rooms::int < 1
   OR r.occupied_rooms::int < 0
   OR r.occupied_rooms::int > r.available_rooms::int;
