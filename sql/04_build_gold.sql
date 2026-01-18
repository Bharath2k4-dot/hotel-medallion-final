CREATE SCHEMA IF NOT EXISTS gold;

-- ===============================
-- 1) Monthly Revenue Aggregation
-- ===============================
DROP TABLE IF EXISTS gold.agg_monthly_revenue;
CREATE TABLE gold.agg_monthly_revenue AS
SELECT
  date_trunc('month', checkin_date)::date AS month,
  COUNT(*) FILTER (WHERE booking_status = 'Confirmed') AS confirmed_bookings,
  COUNT(*) FILTER (WHERE booking_status = 'Cancelled') AS cancelled_bookings,
  COUNT(*) AS total_bookings,

  SUM(total_amount) FILTER (WHERE booking_status = 'Confirmed') AS total_revenue,

  ROUND(
    SUM(total_amount) FILTER (WHERE booking_status = 'Confirmed')
    / NULLIF(COUNT(*) FILTER (WHERE booking_status = 'Confirmed'), 0),
    2
  ) AS adr

FROM silver.bookings
GROUP BY 1
ORDER BY 1;


-- ===============================
-- 2) Monthly Usage / Occupancy
-- ===============================
DROP TABLE IF EXISTS gold.agg_occupancy_monthly;
CREATE TABLE gold.agg_occupancy_monthly AS
SELECT
  date_trunc('month', date)::date AS month,
  SUM(occupied_rooms) AS occupied_rooms,
  SUM(available_rooms) AS available_rooms,

  ROUND(
    SUM(occupied_rooms)::numeric / NULLIF(SUM(available_rooms),0),
    4
  ) AS occupancy_rate
FROM silver.room_inventory
GROUP BY 1
ORDER BY 1;


-- ===============================
-- 3) Wide Dashboard Table
-- ===============================
DROP TABLE IF EXISTS gold.dashboard_monthly;
CREATE TABLE gold.dashboard_monthly AS
SELECT
  r.month,
  r.total_bookings,
  r.confirmed_bookings,
  r.cancelled_bookings,
  ROUND(r.cancelled_bookings::numeric / NULLIF(r.total_bookings,0),4) AS cancellation_rate,

  r.total_revenue,
  r.adr,

  o.occupancy_rate,
  ROUND(r.adr * o.occupancy_rate,2) AS revpar

FROM gold.agg_monthly_revenue r
LEFT JOIN gold.agg_occupancy_monthly o
  ON r.month = o.month
ORDER BY r.month;

