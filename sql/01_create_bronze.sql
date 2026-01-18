CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.hotels_raw;
CREATE TABLE bronze.hotels_raw (
  hotel_id TEXT,
  hotel_name TEXT,
  city TEXT,
  total_rooms INTEGER
);

DROP TABLE IF EXISTS bronze.room_types_raw;
CREATE TABLE bronze.room_types_raw (
  room_type_id TEXT,
  hotel_id TEXT,
  room_type TEXT,
  base_rate NUMERIC
);

DROP TABLE IF EXISTS bronze.bookings_raw;
CREATE TABLE bronze.bookings_raw (
  booking_id TEXT,
  hotel_id TEXT,
  city TEXT,
  room_type TEXT,
  booking_date DATE,
  checkin_date DATE,
  checkout_date DATE,
  nights INTEGER,
  booking_channel TEXT,
  market_segment TEXT,
  booking_status TEXT,
  room_price_per_night NUMERIC,
  total_amount NUMERIC
);

DROP TABLE IF EXISTS bronze.payments_raw;
CREATE TABLE bronze.payments_raw (
  payment_id TEXT,
  booking_id TEXT,
  payment_status TEXT,
  paid_amount NUMERIC,
  payment_date DATE
);

DROP TABLE IF EXISTS bronze.room_inventory_raw;
CREATE TABLE bronze.room_inventory_raw (
  hotel_id TEXT,
  date DATE,
  available_rooms INTEGER,
  occupied_rooms INTEGER
);
