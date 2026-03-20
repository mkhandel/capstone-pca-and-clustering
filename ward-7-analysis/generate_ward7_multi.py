import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# Import exact classification rules
from capture_report import EXACT_RULES

# ---------------- CONFIG ----------------
INPUT_FILES = [
    "SR2018.csv","SR2019.csv","SR2020.csv","SR2021.csv",
    "SR2022.csv","SR2023.csv","SR2024.csv","SR2025.csv","SR2026.csv"
]

OUTPUT_DIR = "ward7_outputs_multi"
WARD_CODE = "07"

CATEGORY_ORDER = [
    "waste","roads","water/sewer","property","noise","trees","animal","other"
]

# ---------------- LOAD ----------------
def load_and_clean_csv(path):
    try:
        df = pd.read_csv(
            path,
            engine="python",
            encoding="utf-8",
            on_bad_lines="skip"
        )
    except UnicodeDecodeError:
        df = pd.read_csv(
            path,
            engine="python",
            encoding="latin1",
            on_bad_lines="skip"
        )

    if df.shape[1] == 10:
        df = df.iloc[:, :9]
    elif df.shape[1] < 9 or df.shape[1] > 10:
        raise ValueError(
            f"{path.name} has {df.shape[1]} columns; expected 9 or 10."
        )
    return df

# ---------------- EXACT CATEGORY ----------------
def apply_exact_categories(df):
    # Apply exact rule matches
    for col, names in EXACT_RULES.items():
        #Take the entire column Service Request Type, 
        # and check for each row whether its value is in names.
        df[col] = df["Service Request Type"].isin(names)

    #convery to Stefanos's preferred format
    def assign_category(row):
        for col in EXACT_RULES.keys():
            # Collapse parking + admin into OTHER (skip bc Stefanos didn't specify)
            if col in ["Is_Parking", "Is_Admin"]:
                continue

            if row[col]:
                #convert internal names to Stefanos's preferred format
                return col.replace("Is_", "").lower().replace("_", "/")

        return "other"

    df["rule_category"] = df.apply(assign_category, axis=1)

    return df

# ---------------- FALLBACK (ONLY FOR UNCOVERED) ----------------
def apply_fallback_regex(df):
    mask = df["rule_category"] == "other"

    type_lower = df.loc[mask, "Service Request Type"].str.lower().fillna("")


    #if condition_1:
    #return "water/sewer"
    df.loc[mask, "rule_category"] = np.select(
        [
            type_lower.str.contains("water|sewer|drain|hydrant"),
            type_lower.str.contains("road|pothole|sidewalk|traffic|snow"),
            type_lower.str.contains("tree|forestry|stump"),
            type_lower.str.contains("animal|dog|cat|wildlife"),
            type_lower.str.contains("noise|sound|music"),
            type_lower.str.contains("property|graffiti|building|fence"),
            type_lower.str.contains("waste|garbage|recycle|bin|litter")
        ],
        [
            "water/sewer",
            "roads",
            "trees",
            "animal",
            "noise",
            "property",
            "waste"
        ],
        default="other"
    )

    return df

# ---------------- PERCENT HELPER ----------------
def pct(series):
    #Force this series to follow this exact list of categories, in this exact order.
    #Adding missing categories → inserts them with NaN
    #also, fillna turns NaNs into 0s
    return (series / series.sum() * 100).reindex(CATEGORY_ORDER).fillna(0)

# ---------------- MAIN ----------------
def main():
    base = Path(".")
    out = base / OUTPUT_DIR
    out.mkdir(exist_ok=True)

    dfs = []

    for f in INPUT_FILES:
        p = base / f
        if p.exists():
            try:
                df = load_and_clean_csv(p)

                df["Creation Date"] = pd.to_datetime(
                    df["Creation Date"], errors="coerce"
                )

                dfs.append(df)
                print(f"Loaded {f} ({len(df)} rows)")

            except Exception as e:
                print(f"⚠️ Failed {f}: {e}")

    df = pd.concat(dfs, ignore_index=True)

    # ---------------- APPLY CATEGORY PIPELINE ----------------
    df = apply_exact_categories(df)
    df = apply_fallback_regex(df)

    # ---------------- FILTER WARD 7 ----------------
    ward_df = df[df["Ward"].astype(str).str.contains(r"\(07\)", na=False)].copy()

    # ---------------- 1. CATEGORY COMPOSITION ----------------
    ward_pct = pct(ward_df["rule_category"].value_counts())

    plt.figure(figsize=(10, 6))
    ax = ward_pct.plot(kind="bar")

    ax.set_title("Ward 7 Service Request Composition by Category")
    ax.set_xlabel("Service Request Category")
    ax.set_ylabel("Percent of total Ward 7 requests (%)")

    for i, v in enumerate(ward_pct):
        ax.text(i, v + 0.5, f"{v:.1f}%", ha='center', fontsize=9)

    plt.xticks(rotation=25, ha="right")
    plt.tight_layout() #adjusts spacing to make sure nothing's overlapping
    plt.savefig(out / "ward7_category.png")
    plt.close()

    # ---------------- 2a. HOUR ----------------
    ward_df["hour"] = ward_df["Creation Date"].dt.hour
    hour_counts = ward_df.groupby("hour").size().reindex(range(24), fill_value=0)

    plt.figure(figsize=(10, 6))
    ax = hour_counts.plot(kind="line", marker="o")

    ax.set_title("Ward 7 Service Requests by Hour of Day")
    ax.set_xlabel("Hour of Day (0–23)")
    ax.set_ylabel("Number of Requests")
    ax.set_xticks(range(24))

    plt.tight_layout()
    plt.savefig(out / "hour.png")
    plt.close()

    # ---------------- 2b. DAY ----------------
    order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

    ward_df["day"] = pd.Categorical(
        ward_df["Creation Date"].dt.day_name(),
        categories=order,
        ordered=True
    )

    day_counts = ward_df.groupby("day", observed=False).size()

    plt.figure(figsize=(10, 6))
    ax = day_counts.plot(kind="bar")

    ax.set_title("Ward 7 Service Requests by Day of Week")
    ax.set_xlabel("Day of Week")
    ax.set_ylabel("Number of Requests")

    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(out / "day.png")
    plt.close()

    # ---------------- 2c. MONTH ----------------
    month_names = ["Jan","Feb","Mar","Apr","May","Jun",
                   "Jul","Aug","Sep","Oct","Nov","Dec"]

    ward_df["month"] = pd.Categorical(
        ward_df["Creation Date"].dt.strftime("%b"),
        categories=month_names,
        ordered=True
    )

    month_counts = ward_df.groupby("month", observed=False).size()

    plt.figure(figsize=(10, 6))
    ax = month_counts.plot(kind="line", marker="o")

    ax.set_title("Ward 7 Monthly Seasonality of Service Requests")
    ax.set_xlabel("Month (Across All Years)")
    ax.set_ylabel("Number of Requests")

    plt.tight_layout()
    plt.savefig(out / "month.png")
    plt.close()

    # ---------------- 3. COMPARISON ----------------
    #ward_pct calculated in category composition section
    city_pct = pct(df["rule_category"].value_counts())
    comp = pd.DataFrame({"Ward 7": ward_pct, "Citywide": city_pct})

    plt.figure(figsize=(11, 6))
    ax = comp.plot(kind="bar")

    ax.set_title("Comparison of Service Request Composition: Ward 7 vs Citywide")
    ax.set_xlabel("Service Request Category")
    ax.set_ylabel("Percent of Requests (%)")

    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()
    plt.savefig(out / "comparison.png")
    plt.close()

    # ---------------- DEBUG METRICS ----------------
    exact_coverage = (df["rule_category"] != "other").mean() * 100
    print(f"\nExact + fallback coverage: {exact_coverage:.2f}%")

    print("\nTop uncaptured types:")
    print(
        df[df["rule_category"] == "other"]["Service Request Type"]
        .value_counts()
        .head(20)
    )

    print("\n✅ All visuals generated in:", OUTPUT_DIR)


# ---------------- RUN ----------------
if __name__ == "__main__":
    main()