import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv("config/.env")

engine = create_engine(os.getenv("DB_URL"))

# Load gold dashboard table
df = pd.read_sql("SELECT * FROM gold.dashboard_monthly", engine)

# 1️⃣ Bookings Trend
plt.figure()
plt.plot(df["month"], df["total_bookings"])
plt.title("Monthly Bookings Trend")
plt.savefig("eda/outputs/bookings_trend.png")

# 2️⃣ Revenue Trend
plt.figure()
plt.plot(df["month"], df["total_revenue"])
plt.title("Monthly Revenue Trend")
plt.savefig("eda/outputs/revenue_trend.png")

# 3️⃣ ADR Trend
plt.figure()
plt.plot(df["month"], df["adr"])
plt.title("ADR Trend")
plt.savefig("eda/outputs/adr_trend.png")

# Save summary
df.to_csv("eda/outputs/dashboard_monthly_summary.csv", index=False)

print("EDA completed")
