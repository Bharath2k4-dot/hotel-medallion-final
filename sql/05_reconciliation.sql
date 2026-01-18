-- Revenue reconciliation
SELECT
  'revenue_check' AS check_name,
  SUM(total_amount) FILTER (WHERE booking_status='Confirmed') AS silver_value,
  (SELECT SUM(total_revenue) FROM gold.agg_monthly_revenue) AS gold_value,
  SUM(total_amount) FILTER (WHERE booking_status='Confirmed')
  - (SELECT SUM(total_revenue) FROM gold.agg_monthly_revenue) AS diff
FROM silver.bookings;

-- Booking count reconciliation
SELECT
  'booking_count_check' AS check_name,
  COUNT(*) AS silver_value,
  (SELECT SUM(total_bookings) FROM gold.agg_monthly_revenue) AS gold_value,
  COUNT(*) - (SELECT SUM(total_bookings) FROM gold.agg_monthly_revenue) AS diff
FROM silver.bookings;
