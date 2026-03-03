import pandas as pd
import numpy as np
from pathlib import Path

# =====================================================
# LOAD RAW CKAN DATA
# =====================================================

def load_ckan_data():
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
        all_dfs.append(temp_df.iloc[:, :9])

    # ⬇️ CONCAT AFTER LOOP (FIXED)
    df = pd.concat(all_dfs, ignore_index=True)

    if df.shape[1] > 9:
        df = df.iloc[:, :9]

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

    df["Creation Date"] = pd.to_datetime(df["Creation Date"], errors="coerce")
    df = df.dropna(subset=["Creation Date", "Service Request Type"])

    return df


# =====================================================
# SHARED FEATURE ENGINEERING
# =====================================================

def add_time_and_category_features(df):

    df["Hour"] = df["Creation Date"].dt.hour
    df["Day_of_Week"] = df["Creation Date"].dt.day_name()

    df["Day_of_Week"] = pd.Categorical(
        df["Day_of_Week"],
        categories=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"],
        ordered=True
    )

    df["Month"] = df["Creation Date"].dt.month
    df["Year"] = df["Creation Date"].dt.year
    df["Is_Weekend"] = df["Creation Date"].dt.weekday >= 5
    df["Is_Night"] = df["Hour"].between(22, 23) | df["Hour"].between(0, 5)

    # Normalize once for safety
    df["type_lower"] = df["Service Request Type"].str.lower()

    # Waste & Sanitation
    df["Is_Waste"] = df["type_lower"].str.contains(
        "garbage|bin|recycle|waste|hazard|storm clean|solid waste",
        na=False
    )

    # Roads & Transportation
    df["Is_Roads"] = df["type_lower"].str.contains(
        "road|pothole|sidewalk|bridge|traffic|debris",
        na=False
    )

    # Water & Sewer
    df["Is_Water_Sewer"] = df["type_lower"].str.contains(
        "water|sewer|drain",
        na=False
    )

    # Property & Bylaw
    df["Is_Property"] = df["type_lower"].str.contains(
        "property|zoning|construction|bylaw|standards",
        na=False
    )

    # Trees & Environment
    df["Is_Trees"] = df["type_lower"].str.contains(
        "pruning|tree",
        na=False
    )

    # Animal Services
    df["Is_Animal"] = df["type_lower"].str.contains(
        "wildlife|animal|stray",
        na=False
    )

    # Noise
    df["Is_Noise"] = df["type_lower"].str.contains(
        "noise|amplified|fireworks",
        na=False
    )

    # Everything else
    df["Is_Other"] = ~(
        df["Is_Waste"] |
        df["Is_Roads"] |
        df["Is_Water_Sewer"] |
        df["Is_Property"] |
        df["Is_Trees"] |
        df["Is_Animal"] |
        df["Is_Noise"]
    )
    
    return df


# =====================================================
# BUILD GEO TABLE (FSA OR WARD)
# =====================================================

def build_geo_table(df, geo_column, min_year=None):

    original_total = len(df)
    working_df = df.copy()

    if min_year:
        pre_2018_rows = (working_df["Creation Date"].dt.year < min_year).sum()

        print(f"----- Pre-{min_year} Filter -----")
        print(f"Total rows before year filter: {original_total:,}")
        print(f"Rows removed (pre-{min_year}): {pre_2018_rows:,}")
        print(f"Percent removed: {pre_2018_rows / original_total * 100:.2f}%")
        print("--------------------------------")

        working_df = working_df[
            working_df["Creation Date"].dt.year >= min_year
        ]

    if geo_column == "First 3 Chars of Postal Code":

        total_rows = len(working_df)

        intersection_rows = (
            working_df[geo_column]
            .astype(str)
            .str.strip()
            .str.upper()
            .eq("INTERSECTION")
            .sum()
        )

        valid_mask = working_df[geo_column].str.match(r"^[A-Z]\d[A-Z]$", na=False)
        valid_rows = valid_mask.sum()
        invalid_rows = total_rows - valid_rows

        print("----- FSA Removal Diagnostics -----")
        print(f"Total rows before FSA filter: {total_rows:,}")
        print(f"Rows labeled INTERSECTION: {intersection_rows:,}")
        print(f"Valid FSA rows: {valid_rows:,}")
        print(f"Invalid / removed rows: {invalid_rows:,}")
        print(f"Percent removed by FSA filter: {invalid_rows / total_rows * 100:.2f}%")
        print("-----------------------------------")

        working_df = working_df[valid_mask]

    working_df = working_df[
        working_df[geo_column].notna()
        & (working_df[geo_column].astype(str).str.strip() != "")
        & (working_df[geo_column].astype(str).str.lower() != "unknown")
    ]

    geo_group = working_df.groupby(geo_column)

    geo_table = geo_group.agg(
        total_requests=("Service Request Type", "count"),
        unique_service_types=("Service Request Type", "nunique"),
        night_ratio=("Is_Night", "mean"),
        weekend_ratio=("Is_Weekend", "mean"),
    )

    geo_table["request_rate"] = geo_table["total_requests"] / len(working_df)

    geo_table["%waste"] = geo_group["Is_Waste"].mean()
    geo_table["%roads"] = geo_group["Is_Roads"].mean()
    geo_table["%water_sewer"] = geo_group["Is_Water_Sewer"].mean()
    geo_table["%property"] = geo_group["Is_Property"].mean()
    geo_table["%trees"] = geo_group["Is_Trees"].mean()
    geo_table["%animal"] = geo_group["Is_Animal"].mean()
    geo_table["%noise"] = geo_group["Is_Noise"].mean()
    geo_table["%other"] = geo_group["Is_Other"].mean()

    daily_counts = working_df.groupby(
        [geo_column, working_df["Creation Date"].dt.date]
    ).size()

    geo_table["avg_requests_per_day"] = daily_counts.groupby(geo_column).mean()

    peak_hour = working_df.groupby(
        [geo_column, "Hour"]
    ).size().reset_index(name="count")

    peak_hour = peak_hour.loc[
        peak_hour.groupby(geo_column)["count"].idxmax()
    ]

    geo_table["peak_hour"] = peak_hour.set_index(geo_column)["Hour"]

    geo_table.reset_index(inplace=True)

    return geo_table


# =====================================================
# MAIN EXECUTION
# =====================================================

if __name__ == "__main__":

    df = load_ckan_data()
    df = add_time_and_category_features(df)
    
    #learning how many types 
    #print("Unique service types:", df["Service Request Type"].nunique())
    #print("\nTop 20 Service Types:")
    #print(df["Service Request Type"].value_counts().head(20))

    # GEO TABLES
    fsa_table = build_geo_table(df, "First 3 Chars of Postal Code")
    ward_table = build_geo_table(df, "Ward", min_year=2018)

    # RESTORED TEMPORAL TABLES
    hour_table = df.groupby("Hour").agg(
        total_requests=("Service Request Type", "count"),
        percent_weekend=("Is_Weekend", "mean"),
        percent_noise=("Is_Noise", "mean"),
        percent_waste=("Is_Waste", "mean"),
        percent_roads=("Is_Roads", "mean"),
        percent_water_sewer=("Is_Water_Sewer", "mean"),
        percent_property=("Is_Property", "mean"),
        percent_trees=("Is_Trees", "mean"),
        percent_animal=("Is_Animal", "mean"),
        percent_other=("Is_Other", "mean"),
        night_indicator=("Is_Night", "mean"),
    ).reset_index()

    dow_table = df.groupby("Day_of_Week").agg(
        total_requests=("Service Request Type", "count"),
        percent_weekend=("Is_Weekend", "mean"),
        percent_noise=("Is_Noise", "mean"),
        percent_waste=("Is_Waste", "mean"),
        percent_roads=("Is_Roads", "mean"),
        percent_water_sewer=("Is_Water_Sewer", "mean"),
        percent_property=("Is_Property", "mean"),
        percent_trees=("Is_Trees", "mean"),
        percent_animal=("Is_Animal", "mean"),
        percent_other=("Is_Other", "mean"),
        night_indicator=("Is_Night", "mean"),
    ).reset_index()

    month_table = df.groupby("Month").agg(
        total_requests=("Service Request Type", "count"),
        percent_weekend=("Is_Weekend", "mean"),
        percent_noise=("Is_Noise", "mean"),
        percent_waste=("Is_Waste", "mean"),
        percent_roads=("Is_Roads", "mean"),
        percent_water_sewer=("Is_Water_Sewer", "mean"),
        percent_property=("Is_Property", "mean"),
        percent_trees=("Is_Trees", "mean"),
        percent_animal=("Is_Animal", "mean"),
        percent_other=("Is_Other", "mean"),
        night_indicator=("Is_Night", "mean"),
    ).reset_index()

    def build_service_table(df, geo_column):
        return df.groupby("Service Request Type").agg(
            total_frequency=("Service Request Type", "count"),
            geo_coverage=(geo_column, "nunique"),
            percent_weekend=("Is_Weekend", "mean"),
        ).reset_index()
        
    service_table_fsa = build_service_table(df, "First 3 Chars of Postal Code")
    service_table_ward = build_service_table(df, "Ward")

    with pd.ExcelWriter("CKAN_Feature_Tables.xlsx", engine="xlsxwriter") as writer:
        fsa_table.to_excel(writer, sheet_name="FSA", index=False)
        ward_table.to_excel(writer, sheet_name="Ward", index=False)
        hour_table.to_excel(writer, sheet_name="Hour", index=False)
        dow_table.to_excel(writer, sheet_name="Day_of_Week", index=False)
        month_table.to_excel(writer, sheet_name="Month", index=False)
        service_table_fsa.to_excel(writer, sheet_name="Service_Type_FSA", index=False)
        service_table_ward.to_excel(writer, sheet_name="Service_Type_Ward", index=False)


    print("Feature tables created successfully.")