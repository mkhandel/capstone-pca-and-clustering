import pandas as pd
import numpy as np
from pathlib import Path

# ----------------------------
# Load ALL raw CKAN CSVs
# ----------------------------

data_path = Path("data")
csv_files = list(data_path.glob("*.csv"))

if len(csv_files) == 0:
    raise FileNotFoundError("No CSV files found in /data folder")

all_dfs = []

for file in csv_files:
    print(f"Loading {file.name}...")
    
    temp_df = pd.read_csv(
        file,
        encoding="latin1",
        engine="python",
        usecols=range(9)
    )

    if temp_df.shape[1] > 9:
        temp_df = temp_df.iloc[:, :9]

    all_dfs.append(temp_df)

df = pd.concat(all_dfs, ignore_index=True)

print(f"Combined dataset BEFORE trimming: {df.shape}")

# Force exactly 9 columns safely
if df.shape[1] > 9:
    df = df.iloc[:, :9]

print(f"Combined dataset AFTER trimming: {df.shape}")

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

total_rows_initial = len(df)

# Count pre-2018 rows BEFORE filtering
pre_2018_rows = (df["Creation Date"].dt.year < 2018).sum()

print("----- Pre-2018 Filter -----")
print(f"Total rows before year filter: {total_rows_initial:,}")
print(f"Rows removed (pre-2018 ward structure): {pre_2018_rows:,}")
print(f"Percent removed: {pre_2018_rows / total_rows_initial * 100:.2f}%")
print("--------------------------------")

# Now apply 2018+ filter
df = df[df["Creation Date"].dt.year >= 2018]

# ---- Ward diagnostics AFTER year filter ----
total_rows_post_year = len(df)

missing_ward = df["Ward"].isna().sum()
unknown_ward = df["Ward"].astype(str).str.strip().str.lower().eq("unknown").sum()
empty_ward = df["Ward"].astype(str).str.strip().eq("").sum()

valid_mask = (
    df["Ward"].notna()
    & (df["Ward"].astype(str).str.strip() != "")
    & (df["Ward"].astype(str).str.strip().str.lower() != "unknown")
)

valid_rows = valid_mask.sum()
invalid_rows = total_rows_post_year - valid_rows

print("----- Ward Removal Diagnostics -----")
print(f"Total rows (post-2018): {total_rows_post_year:,}")
print(f"Missing Ward: {missing_ward:,}")
print(f"Unknown Ward: {unknown_ward:,}")
print(f"Empty Ward: {empty_ward:,}")
print(f"Invalid / removed rows: {invalid_rows:,}")
print(f"Percent removed: {invalid_rows / total_rows_post_year * 100:.2f}%")
print("------------------------------------")

df = df[valid_mask]
df = df.dropna(subset=["Creation Date", "Service Request Type"])

# ----------------------------
# Time Features
# ----------------------------
df["Hour"] = df["Creation Date"].dt.hour
df["Day_of_Week"] = df["Creation Date"].dt.day_name()
#put days in chronological order
df["Day_of_Week"] = pd.Categorical(
    df["Day_of_Week"],
    categories=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"],
    ordered=True
)
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