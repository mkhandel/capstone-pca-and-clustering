import pandas as pd
import numpy as np

# ----------------------------
# Load raw CKAN CSV
# ----------------------------

# Read with python engine (handles irregular rows)
df = pd.read_csv(
    "SR2026.csv",
    encoding="latin1",
    engine="python",
    usecols=range(9)   # <-- Only read first 9 columns
)

# If file has extra column(s), keep only first 9
if df.shape[1] > 9:
    df = df.iloc[:, :9]

# Rename columns explicitly to ensure consistency
df.columns = [
    "Creation Date",
    "Status",
    "First 3 Chars of Postal Code",
    "Intersection Street 1",
    "Intersection Street 2",
    "Ward",
    "Service Request Type",
    "Division",
    "Section"
]

# ----------------------------
# Basic Cleaning
# ----------------------------
df["Creation Date"] = pd.to_datetime(df["Creation Date"], errors="coerce")
df = df.dropna(subset=["Creation Date", "Ward", "Service Request Type"])

# ----------------------------
# Time Features
# ----------------------------
df["Hour"] = df["Creation Date"].dt.hour
df["Day_of_Week"] = df["Creation Date"].dt.day_name()
df["Month"] = df["Creation Date"].dt.month
df["Is_Weekend"] = df["Creation Date"].dt.weekday >= 5
df["Is_Night"] = df["Hour"].between(22, 23) | df["Hour"].between(0, 5)

# ----------------------------
# Service Category Flags
# ----------------------------
df["Is_Noise"] = df["Service Request Type"].str.contains("noise", case=False, na=False)
df["Is_Water"] = df["Service Request Type"].str.contains("water", case=False, na=False)
df["Is_Transport"] = df["Service Request Type"].str.contains("transport|traffic", case=False, na=False)

df["Is_Other"] = ~(df["Is_Noise"] | df["Is_Water"] | df["Is_Transport"])

# ----------------------------
# 1️⃣ WARD TABLE
# ----------------------------
ward_group = df.groupby("Ward")

ward_table = ward_group.agg(
    total_requests=("Service Request Type", "count"),
    unique_service_types=("Service Request Type", "nunique"),
    night_ratio=("Is_Night", "mean"),
    weekend_ratio=("Is_Weekend", "mean"),
)

ward_table["request_rate"] = ward_table["total_requests"] / len(df)

ward_table["%noise"] = ward_group["Is_Noise"].mean()
ward_table["%water"] = ward_group["Is_Water"].mean()
ward_table["%transport"] = ward_group["Is_Transport"].mean()
ward_table["%other"] = ward_group["Is_Other"].mean()

daily_counts = df.groupby(["Ward", df["Creation Date"].dt.date]).size()
ward_table["avg_requests_per_day"] = daily_counts.groupby("Ward").mean()

peak_hour = df.groupby(["Ward", "Hour"]).size().reset_index(name="count")
peak_hour = peak_hour.loc[peak_hour.groupby("Ward")["count"].idxmax()]
ward_table["peak_hour"] = peak_hour.set_index("Ward")["Hour"]

ward_table.reset_index(inplace=True)

# ----------------------------
# 2️⃣ HOUR TABLE
# ----------------------------
hour_group = df.groupby("Hour")

hour_table = hour_group.agg(
    total_requests=("Service Request Type", "count"),
    percent_weekend=("Is_Weekend", "mean"),
    percent_noise=("Is_Noise", "mean"),
    percent_water=("Is_Water", "mean"),
    percent_transport=("Is_Transport", "mean"),
    percent_other=("Is_Other", "mean"),
    night_indicator=("Is_Night", "mean"),
)

hour_ward = df.groupby(["Hour", "Ward"]).size()
hour_table["avg_requests_per_ward"] = hour_ward.groupby("Hour").mean()

hour_table.reset_index(inplace=True)

# ----------------------------
# 3️⃣ DAY OF WEEK TABLE
# ----------------------------
dow_group = df.groupby("Day_of_Week")

dow_table = dow_group.agg(
    total_requests=("Service Request Type", "count"),
    percent_noise=("Is_Noise", "mean"),
    percent_water=("Is_Water", "mean"),
    percent_transport=("Is_Transport", "mean"),
    percent_other=("Is_Other", "mean"),
    weekend_indicator=("Is_Weekend", "mean"),
)

dow_ward = df.groupby(["Day_of_Week", "Ward"]).size()
dow_table["avg_requests_per_ward"] = dow_ward.groupby("Day_of_Week").mean()

dow_table.reset_index(inplace=True)

# ----------------------------
# 4️⃣ MONTH TABLE
# ----------------------------
month_group = df.groupby("Month")

month_table = month_group.agg(
    total_requests=("Service Request Type", "count"),
    percent_noise=("Is_Noise", "mean"),
    percent_water=("Is_Water", "mean"),
    percent_transport=("Is_Transport", "mean"),
)

month_ward = df.groupby(["Month", "Ward"]).size()
month_table["avg_requests_per_ward"] = month_ward.groupby("Month").mean()

month_table.reset_index(inplace=True)

# ----------------------------
# 5️⃣ SERVICE TYPE TABLE
# ----------------------------
service_group = df.groupby("Service Request Type")

service_table = service_group.agg(
    total_frequency=("Service Request Type", "count"),
    wards_active=("Ward", "nunique"),
    percent_weekend=("Is_Weekend", "mean"),
)

service_ward = df.groupby(["Service Request Type", "Ward"]).size()
service_table["avg_requests_per_ward"] = service_ward.groupby("Service Request Type").mean()

service_table.reset_index(inplace=True)

# ----------------------------
# Export to Excel
# ----------------------------
with pd.ExcelWriter("CKAN_Feature_Tables.xlsx", engine="xlsxwriter") as writer:
    ward_table.to_excel(writer, sheet_name="Ward", index=False)
    hour_table.to_excel(writer, sheet_name="Hour", index=False)
    dow_table.to_excel(writer, sheet_name="Day_of_Week", index=False)
    month_table.to_excel(writer, sheet_name="Month", index=False)
    service_table.to_excel(writer, sheet_name="Service_Type", index=False)

print("Excel file with 5 feature tables created successfully.")