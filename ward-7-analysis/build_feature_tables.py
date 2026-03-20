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

    # Better Normalization
    df["type_lower"] = (
    df["Service Request Type"]
    .str.lower()
    .str.replace("-", " ")
    .str.replace("/", " ")
)

    # Waste & Sanitation
    df["Is_Waste"] = df["type_lower"].str.contains(
        r"garbage|bin|recycle|waste|hazard|storm clean|solid waste|dump|litter|collection|pickup|appliance|furniture|cart|yard waste|white goods|pickup request|not picked up|needles|excrement|pollution",
        na=False
    )

    # Roads & Transportation
    df["Is_Roads"] = df["type_lower"].str.contains(
        r"road|pothole|sidewalk|bridge|traffic|debris|snow|salting|salt|icy|plow|plough|bollards|windrow|curb|asphalt|lane|sign|signal|streetlight|pavement|walkway|sink hole|culvert|maintenance hole|expressway|guardrail|intersection|driveway|parking|bollard|pxo",
        na=False
    )

    # Water & Sewer
    df["Is_Water_Sewer"] = df["type_lower"].str.contains(
        r"water|sewer|drain|flood|hydrant|leak|basin|catch basin|stormwater",
        na=False
    )

    # Property & Bylaw
    df["Is_Property"] = df["type_lower"].str.contains(
        r"property standards|property|permit|zoning|construction|by law|bylaw|standards|building|fence|graffiti",
        na=False
    )

    # Environment
    df["Is_Environment"] = df["type_lower"].str.contains(
        r"tree|pruning|branch|fallen tree|stump|forestry|grass|weed|weeds|leaf|stemming",
        na=False
    )

    # Animal Services
    df["Is_Animal"] = df["type_lower"].str.contains(
        r"animal|wildlife|stray|raccoon|dog|cat|dead animal|injured animal|bee|wasp|hornet|hive|cadaver|injured|moth",
        na=False
    )

    # Noise
    df["Is_Noise"] = df["type_lower"].str.contains(
        r"noise|amplified|music|party|loud|fireworks",
        na=False
    )
    
    #Admin
    df["Is_Admin"] = df["type_lower"].str.contains(
    r"complaint|compliment|staff|operator|operations|timeline|service complaint|suggestion|contractor complaint",
    na=False
    )

    # Everything else
    df["Is_Other"] = ~(
        df["Is_Waste"] |
        df["Is_Roads"] |
        df["Is_Water_Sewer"] |
        df["Is_Property"] |
        df["Is_Environment"] |
        df["Is_Animal"] |
        df["Is_Noise"] |
        df["Is_Admin"]
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
    geo_table["%environmental"] = geo_group["Is_Environment"].mean()
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
    
    
    # =====================================================
    # SERVICE TYPE ANALYSIS (FOR BETTER BUCKETING)
    # =====================================================
    
    #Printing out Different Category Counts and Percentages
    print("\nCategory Counts:")

    categories = [
        "Is_Waste",
        "Is_Roads",
        "Is_Water_Sewer",
        "Is_Property",
        "Is_Environment",
        "Is_Animal",
        "Is_Noise",
        "Is_Other"
        ]

    for c in categories:
        count = df[c].sum()
        percent = count / len(df) * 100
        print(f"{c}: {count:,} ({percent:.2f}%)")
    
    # =====================================================
    # TOP CONTRIBUTORS TO EACH CATEGORY
    # =====================================================

    print("\nTop contributors to each category:\n")

    categories = [
        "Is_Waste",
        "Is_Roads",
        "Is_Water_Sewer",
        "Is_Property",
        "Is_Environment",
        "Is_Animal",
        "Is_Noise"
    ]

    top_contributors = {}

    for cat in categories:
        print(f"\nTop 25 service types in {cat}:\n")
        
        top = (
            df[df[cat] == 1]["Service Request Type"]
            .value_counts()
            .head(25)
        )
        
        print(top)
        
        # store for Excel export
        top_contributors[cat] = top.reset_index()
        top_contributors[cat].columns = ["Service Request Type", "Count"]

    # Save to Excel
    with pd.ExcelWriter("Top_Service_Type_Contributors_By_Category.xlsx") as writer:
        for cat, table in top_contributors.items():
            table.to_excel(writer, sheet_name=cat, index=False)
            
    
    print("\nTotal unique service request types:", df["Service Request Type"].nunique())

    service_counts = df["Service Request Type"].value_counts()

    print("\nTop 1000 Service Request Types:\n")
    print(service_counts.head(1000))

    service_counts.head(1000).to_excel(
        "Top_1000_Service_Request_Types.xlsx",
        sheet_name="Top_Service_Types"
    )

    top1000_total = service_counts.head(1000).sum()
    coverage = top1000_total / len(df) * 100

    print(f"\nTop 1000 types cover {coverage:.2f}% of all requests")
    
    

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
        percent_trees=("Is_Environment", "mean"),
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
        percent_trees=("Is_Environment", "mean"),
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
        percent_trees=("Is_Environment", "mean"),
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