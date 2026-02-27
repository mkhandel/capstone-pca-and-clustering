import pandas as pd
import matplotlib.pyplot as plt
from build_feature_tables import load_ckan_data, add_time_and_category_features

# Load full dataset
df = load_ckan_data()
df = add_time_and_category_features(df)

# =========================
# 1. Requests Per Year
# =========================
year_counts = df.groupby("Year").size()

plt.figure(figsize=(10,6))
year_counts.plot(marker="o")
plt.title("Total 311 Requests per Year")
plt.ylabel("Total Requests")
plt.grid(True)
plt.tight_layout()
plt.savefig("temporal_requests_per_year.png", dpi=200)
plt.close()

# =========================
# 2. Requests Per Month (All Years Combined)
# =========================
month_counts = df.groupby("Month").size()

plt.figure(figsize=(10,6))
month_counts.plot(marker="o")
plt.title("Total Requests by Month (All Years Combined)")
plt.ylabel("Total Requests")
plt.grid(True)
plt.tight_layout()
plt.savefig("temporal_requests_by_month.png", dpi=200)
plt.close()

# =========================
# 3. Requests Per Hour
# =========================
hour_counts = df.groupby("Hour").size()

plt.figure(figsize=(10,6))
hour_counts.plot(kind="bar")
plt.title("Requests by Hour of Day")
plt.ylabel("Total Requests")
plt.tight_layout()
plt.savefig("temporal_requests_by_hour.png", dpi=200)
plt.close()

# =========================
# 4. Weekend vs Weekday
# =========================
weekend_counts = df.groupby("Is_Weekend").size()

plt.figure(figsize=(6,6))
weekend_counts.plot(kind="bar")
plt.title("Weekend vs Weekday Requests")
plt.xticks([0,1], ["Weekday", "Weekend"], rotation=0)
plt.ylabel("Total Requests")
plt.tight_layout()
plt.savefig("temporal_weekend_vs_weekday.png", dpi=200)
plt.close()

print("Temporal visualizations saved.")