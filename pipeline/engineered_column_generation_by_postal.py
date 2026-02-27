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
df = df.iloc[:,:9]

print(f"Combined dataset BEFORE shape: {df.shape}")

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

df = df.dropna(subset=["Creation Date", "Service Request Type"])

df["First 3 Chars of Postal Code"] = (
    df["First 3 Chars of Postal Code"].astype(str).str.strip().str.upper()
)

total_rows = len(df)
intersection_rows = (df["First 3 Chars of Postal Code"] == "INTERSECTION").sum()

valid_fsa_mask = df["First 3 Chars of Postal Code"].str.match(r"^[A-Z]\d[A-Z]$", na=False)
valid_fsa_rows = valid_fsa_mask.sum()

invalid_rows = total_rows - valid_fsa_rows

print("----- FSA Removal Diagnostics -----")
print(f"Total usable rows: {total_rows:,}")
print(f"Rows labeled INTERSECTION: {intersection_rows:,}")
print(f"Valid FSA rows: {valid_fsa_rows:,}")
print(f"Invalid / non-FSA rows: {invalid_rows:,}")
print(f"Percent removed by FSA filter: {invalid_rows / total_rows * 100:.2f}%")
print("-----------------------------------")

df = df[valid_fsa_mask]
print(f"Rows remaining after FSA filter: {len(df):,}")


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
# 1️⃣ FSA TABLE
# ----------------------------
fsa_group = df.groupby("First 3 Chars of Postal Code")

fsa_table = fsa_group.agg(
    total_requests=("Service Request Type", "count"),
    unique_service_types=("Service Request Type", "nunique"),
    night_ratio=("Is_Night", "mean"),
    weekend_ratio=("Is_Weekend", "mean"),
)

fsa_table["request_rate"] = fsa_table["total_requests"] / len(df)

fsa_table["%noise"] = fsa_group["Is_Noise"].mean()
fsa_table["%water"] = fsa_group["Is_Water"].mean()
fsa_table["%transport"] = fsa_group["Is_Transport"].mean()
fsa_table["%other"] = fsa_group["Is_Other"].mean()

daily_counts = df.groupby(
    ["First 3 Chars of Postal Code", df["Creation Date"].dt.date]
).size()

fsa_table["avg_requests_per_day"] = daily_counts.groupby(
    "First 3 Chars of Postal Code"
).mean()

peak_hour = df.groupby(
    ["First 3 Chars of Postal Code", "Hour"]
).size().reset_index(name="count")

peak_hour = peak_hour.loc[
    peak_hour.groupby("First 3 Chars of Postal Code")["count"].idxmax()
]

fsa_table["peak_hour"] = peak_hour.set_index(
    "First 3 Chars of Postal Code"
)["Hour"]

fsa_table.reset_index(inplace=True)

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

hour_fsa = df.groupby(["Hour", "First 3 Chars of Postal Code"]).size()
hour_table["avg_requests_per_fsa"] = hour_fsa.groupby("Hour").mean()

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

dow_fsa = df.groupby(["Day_of_Week", "First 3 Chars of Postal Code"]).size()
dow_table["avg_requests_per_fsa"] = dow_fsa.groupby("Day_of_Week").mean()

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

month_fsa = df.groupby(["Month", "First 3 Chars of Postal Code"]).size()
month_table["avg_requests_per_fsa"] = month_fsa.groupby("Month").mean()

month_table.reset_index(inplace=True)

# ----------------------------
# 5️⃣ SERVICE TYPE TABLE
# ----------------------------
service_group = df.groupby("Service Request Type")

service_table = service_group.agg(
    total_frequency=("Service Request Type", "count"),
    fsas_active=("First 3 Chars of Postal Code", "nunique"),
    percent_weekend=("Is_Weekend", "mean"),
)

service_fsa = df.groupby(
    ["Service Request Type", "First 3 Chars of Postal Code"]
).size()

service_table["avg_requests_per_fsa"] = service_fsa.groupby(
    "Service Request Type"
).mean()

service_table.reset_index(inplace=True)

# ----------------------------
# Export to Excel
# ----------------------------
with pd.ExcelWriter("CKAN_Feature_Tables.xlsx", engine="xlsxwriter") as writer:
    fsa_table.to_excel(writer, sheet_name="FSA", index=False)
    hour_table.to_excel(writer, sheet_name="Hour", index=False)
    dow_table.to_excel(writer, sheet_name="Day_of_Week", index=False)
    month_table.to_excel(writer, sheet_name="Month", index=False)
    service_table.to_excel(writer, sheet_name="Service_Type", index=False)

print("Excel file with 5 feature tables created successfully.")