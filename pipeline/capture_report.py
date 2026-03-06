import pandas as pd
from pathlib import Path

# =====================================================
# EXACT RULE BUCKETS
# Mirrors MEASURE_EXTRACTION_RULES in config.py exactly.
# Keep this in sync with config.py when rules change.
# =====================================================


EXACT_RULES = {
    "Is_Waste": {
        "Residential Furniture / Not Picked Up",
        "Residential: Bin: Repair or Replace Lid",
        "Res / Garbage / Not Picked Up",
        "Res / Organic Bin / Replace Damaged",
        "Storm Clean Up",
        "Res / Recycle / Not Picked Up",
        "Res / Organic Green Bin / Not Picked Up",
        "Residential: Bin: Repair or Replace Body/Handle",
        "Publication Request - Solid Waste",
        "Residential: Garbage Bin: Exchange to Medium",
        "All / Hazardous Waste / Pick Up Request",
        "Waste",
        "Residential:Recycle Bin:Exchange to Extra Large",
        "Residential: Garbage Bin: Exchange to Small",
        "Residential / Yard Waste / Not Picked Up",
        "Res / Organic Bin / Replace Missing",
        "Residential: Garbage Bin: Exchange to Large",
        "Residential: Garbage Bin: Missing",
        "Residential: Recycle Bin: Missing",
        "Bin Investigation Request",
        "Residential: Recycle Bin: Exchange to Large",
        "Litter / Sidewalk & Blvd / Pick Up Request",
        "Residential: Recycle Bin: Additional Extra Large",
        "Res / Organic Green Bin / Multiple Addresses / Not Picked Up",
        "Litter / Illegal Dumping Cleanup",
        "Res / Organic Bin / Additional",
        "Residential Bin Lid Damaged",
        "Residential: Garbage Bin: Exchange to Extra Large",
        "Litter / Bin / Overflow or Not Picked Up",
        "All / White Goods / Pick Up Request",
        "Res / Yard Waste Multiple Addresses / Not Picked Up",
        "Residential: Bin: Wrong Delivery",
        "Illegal Dumping",
        "Residential: Bin: Repair or Replace Wheel",
        "Residential: Recycle Bin: Exchange to Medium",
        "Res / Recycle / Multiple Addresses / Not Picked Up",
        "Res / Garbage / Multiple Addresses Not Picked Up",
        "Multi-Res / Garbage Front-End / Not Picked Up",
        "Residential: Bin: Missing",
        "Litter / Laneway / Clean Up",
        "Residential Bin Body or Handle Damaged",
        "Res / Organic Bin / New Occupants",
        "Res / Organic&Garbage / Not Picked Up",
        "Residential: Recycle Bin: Additional Large",
        "Res / Organic Bin / New Account",
        "FEL Multi-Res Furniture / Not Picked Up",
        "Multi-Res / Recycle Front-End / Not Picked Up",
        "Res / Organic&Recycle / Not Picked Up",
        "Pollution Spill Response",
        "Clean up Illegal Dumping on City Road Allowance",
        "Multi-Res / Garbage Cart / Not Picked Up",
        "Multi-Res / Furniture Pile / Not Picked Up",
        "Multi-Res / Recycle Cart / Not Picked Up",
        "Spills/Cleanup/Collections Curb Day",
        "Res / Org&Recycle Multiple Addresses / Not Picked Up",
        "Residential: Recycle Bin: New Account Large",
        "Hazardous Waste Pick-up",
        "Residential: Garbage Bin: New Account Medium",
        "Res / Organic Green Bin / Retrieval",
        "Residential: Garbage Bin: Additional Extra Large",
        "Waste Storage",
        "Res / Organic Front&Side / Not Picked Up",
        "Residential: Recycle Bin: New Account Extra Large",
        "Res Above Comm / Nite Garbage / Not Picked Up",
        "Residential: Recycle Bin: Exchange to Small",
        "Prohibited Waste",
        "Res / Garbage Front&Side / Not Picked Up",
        "Res / Recycle Front&Side / Not Picked Up",
        "Res / Org&Garbage Multiple Addresses / Not Picked Up",
        "Multi-Res / Organic FEL / Not Picked Up",
        "FEL Non-Res / Garbage / Not Picked Up",
        "Replace Missing Residential Organic Bin",
        "Litter/Needle Cleanup",
        "Residential: Garbage Bin: New Account Small",
        "Residential: Recycle Bin: New Account Medium",
        "Residential: Recycle Bin: Additional Medium",
        "Multi-residential Front End Loaded Oversized Items Not Picked Up",
        "Non-Res Recycle Bin / Not Picked Up",
        "Res / Nite Garbage / Not Picked Up",
        "Res Above Comm / Nite Recycle / Not Picked Up",
        "Non-Res Organic Bin Nite / Not Picked Up",
        "Contaminated Waste/Preparation",
        "Residential: Garbage Bin: Additional Large",
        "Residential: Bin: Repair or Replace Metal Bar",
        "Residential: Bin: Wrong Delivery or Bin Return",
        "Waste Set Out - Wrong Location / Time/ Day",
        "Additional Residential Organic Bin",
        "Res / Nite Organic / Not Picked Up",
        "Multi-Res / Organic Bin / Not Picked Up",
        "Res / Org&Garbage Front&Side / Not Picked Up",
        "FEL Multi-Res / Recycle Cart / Not Picked Up",
        "Res / Org&Recycle Front&Side / Not Picked Up",
        "Containers",
        "FEL Non-Res Recycle FEL / Not Picked Up",
        "Non-Res Garbage Bin / Not Picked Up",
        "Garbage / Park / Bin Overflow",
        "Non-Res Organic Bin / Not Picked Up",
        "Residential Bin Wheel Damaged",
        "Residential / Nite Furniture / Not Picked Up",
        "Comm / Nite Organic Cart / Not Picked Up",
        "Illegal Dumping / Discharge",
        "Illegal Dumping on Road",
        "Illegal Dumping on Roadside",
        "Non-Res Garbage Bin Nite / Not Picked Up",
        "Non-Res Recycle Bin Nite / Not Picked Up",
        "Residential: Garbage Bin: New Account Extra Large",
        "Res / Nite Recycle / Not Picked Up",
        "Res / Above Comm / Organic Green Bin / Not Picked Up",
        "Curb Day Collection - Complaint - Solid Waste Management",
        "Curb Day Collection Staff - Complaint - Solid Waste Management",
        "Clean up Litter on Sidewalks and Boulevards",
        "Boulevard - Pick-Up Shopping Carts",
        "Res / Organic Green Bin / Inquiry",
        "Residential: Recycle Bin: Exchange to Extra Large",
        "Residential: Garbage Bin: Additional Medium",
        "Complaint / Investigation - Leaves",
        "Dispute SR Status/Bins",
    },
    "Is_Roads": {
        "Road - Pot hole",
        "Road - Cleaning/Debris",
        "Missing/Damaged Signs",
        "Traffic Signal Maintenance",
        "Sidewalk - Damaged / Concrete",
        "Road - Sinking",
        "Litter / Sidewalk & Blvd / Pick Up Request",
        "Boulevards - Damaged Asphalt",
        "Ice and Snow Complaint",
        "Road - Damaged",
        "Signs",
        "Road Ploughing Required",
        "Sidewalk - Snow Clearing",
        "Boulevard - Plough Damage",
        "Driveway-Blocked By Windrow",
        "Sidewalk - Cleaning",
        "Sidewalk Snow Clearing Required",
        "Road - Sanding / Salting Required",
        "Sidewalk Icy|| Needs Sand/Salt",
        "Roadway Utility Cut - Settlement",
        "Road-Winter Request/ Complaint",
        "Road Pothole / Road Damage",
        "Road Plowing Request",
        "Driveway - Damaged / Ponding",
        "Driveway Blocked By Plowed Snowbank",
        "Maintenance Holes -Damage / Repair",
        "PXO Maintenance",
        "Investigate Regulatory Signs",
        "Maintenance Holes Lid Loose/Missing",
        "Curb - Damaged",
        "Missing/Faded Pavement Markings",
        "Laneway - Surface Damage",
        "Clean up Debris on Road",
        "Curb - Adjust Height (Too High/Low)",
        "Signal Timing Review/Vehicle Delays",
        "Traffic Signal Repair",
        "Missing / Damaged Street or Traffic Signs",
        "Sight Line Obstruction",
        "Maintenance Hole-Damage",
        "Expressway requires cleaning.",
        "Sidewalk - Damaged /Brick/Interlock",
        "Sink Hole",
        "Walkway - damaged",
        "Snow Removal - General",
        "Icy Sidewalk Needs Salting",
        "Culverts - Blocked",
        "Boulevards - Snow Piled Too High / Too Much",
        "Intersection Safety Review",
        "Snow Removal - Sightline Problem",
        "Damaged Concrete Sidewalk",
        "Investigate Warning Signs",
        "Maintenance Hole - Overflowing",
        "Laneway - Salting / Sanding / Salt",
        "Investigate Pavement Markings",
        "Pedestrian Issues/Timing/Delays",
        "Investigate Temporary Condition Signs",
        "Roadside - Plough Damage",
        "Sidewalk - Seniors Snow Clearing",
        "Road Salting Request",
        "Culverts-Damaged / Maintenance Requested",
        "Illegal Snow Dumping & Failure to Clear Snow or Ice on Public Sidewalk",
        "Pot hole on Expressway",
        "New Traffic Control Signal Request",
        "Boulevard Plow Damage",
        "Bollard - Damaged",
        "Traffic Calming Measures",
        "Bus Stops Snow Clearing Required",
        "Roadside Utility Cut - Settlement",
        "Left/Right Turn Signal Priority Features",
        "New Pedestrian Crossover",
        "Snow at Intersections - Impeded Mobility",
        "Laneway Needs Salting",
        "Ditch Maintenance Requested",
        "Disabled Persons' Parking Space",
        "Walkway - Snow Clearing/ Salting required",
    },
    "Is_Water_Sewer": {
        "Sewer Service Line-Blocked",
        "Water Service Line-Turn Off",
        "Water Service Line-Turn On",
        "Watermain-Possible Break",
        "Catch Basin - Blocked / Flooding",
        "Water Service Line-No Water",
        "Water Service Line-Leaking",
        "Water Service Line-Check Water Service Box",
        "Registration - Toronto Water",
        "Water Service Line-Turn Off/Burst",
        "Water Service Line-Low Pressure|| Low Flow",
        "Hydrant-Leaking",
        "Catch basin (Storm) - Overflowing",
        "Water Service Line-Low Pressure|| Low Flow Appt",
        "Water Service Line-Locate / Adjust service box",
        "Sewer Odour",
        "Water Service Line-Damaged Water Service Box",
        "Road Water Ponding",
        "Catch Basin - Debris / Litter",
        "Catch basin (Storm) - Damage",
        "Catch basin Maintenance and Repair",
        "Water Service Test for High Lead Content",
        "Hydrant-Damage",
        "Sidewalk-Water Ponding",
        "Sewer Service Line-Cleanout Repair",
        "Water Valve-Leaking",
        "Water Service Line-Low Pressure|| Low Flow Insp",
        "Water Quality-Discoloured (Rusty or dirty) Water",
        "Watermain Valve - Turn Off",
        "Request Water Service Turn Off",
        "Request Water Service Turn On",
        "Watermain Valve - Turn On",
        "Water Service Line - Low Pressure|| Low Flow - Ongoing",
        "Water Service Line - Low Pressure|| Low Flow Inspection - (Sudden)",
        "Sewer main-Backup",
        "Catch Basin - Damaged Maintenance Requested",
        "Salting-Winter (WSL/HYDT/VALVE/Watermain Break Locations etc.)",
        "Water Meter-Leaking",
        "Catch basin (Storm) - Other",
        "Hydrant-After Usage Test",
        "Catch Basin -Cover Missing / Damaged / Loose",
        "Complaint / Investigation - Water Discharge",
        "Locate-Emergency",
    },
    "Is_Property": {
        "Property Standards",
        "Zoning",
        "Graffiti",
        "Property Standards and Maintenance Violations",
        "Construction Noise",
        "By-Law Contravention Invest",
        "Fence",
        "Postering City Property/Structures",
        "Sidewalk - Graffiti Complaint",
        "Zoning Regulations Violations",
        "Waste or Illegal Dumping on Private Property",
        "Report an Encroachment on City Property",
        "Long Grass and Prohibited Plants on Private Property",
        "Bridge - Graffiti Complaint",
        "Permit Inspection",
        "Traffic Signal - Graffiti Complaint",
        "Road - Graffiti Complaint",
        "Bylaw Enforcement: Excavation",
        "Fence - Damaged",
        "Postering City Property / Structures",
        "Traffic Sign - Graffiti Complaint",
        "Graffiti on Private Property",
        "Illegal Dumping on City Property",
        "Traffic Signal Equipment - Graffiti Complaint",
        "Complaint/Investigation - Encroachment",
        "Adequate Heat",
        "Restoration Related",
        "Election Signs",
        "Park Use",
        "Complaint / Investigation - Idling Enforcement",
        "ENF/INVEST EXCREMENT",
        "IPM Inspection",
        "ENF/INVEST UNSAN COND",
    },
    "Is_Environment": {
        "General Pruning",
        "Long Grass and Weeds",
        "Stemming",
        "General Tree Maintenance",
        "Tree Planting",
        "Dangerous Private Tree Investigation",
        "Residential / XMAS Tree / Not Picked Up",
        "Boulevards-Grass Cutting",
        "Tree Emergency Clean-Up",
        "MLS Hazard Tree Invst",
        "Long Grass and Prohibited Plants on Private Property",
        "Sewer Service Line-Tree Root Reimbursement",
        "Boulevards - Weed Removal",
        "Complaint / Investigation - Grass and Weeds Enforcement",
        "Walkway-Weeds Need Cutting",
        "Private Tree Inspection",
        "Residential or Park Tree Removal",
        "Commercial Tree Pruning",
        "Traffic Island-Grass Needs Cutting",
        "Unauthorized Tree Injury or Removal",
        "EAB Exemption Request",
        "Stumping",
        "Boulevard - Leaf Pick-up Mechanical",
        "Street Light Out",
    },
    "Is_Animal": {
        "CADAVER WILDLIFE",
        "INJUR/DIST WILDLIFE",
        "Injured - Wildlife",
        "Cadaver - Wildlife",
        "Pick up Dead Wildlife",
        "CADAVER DOMESTIC",
        "STRAY AT LARGE",
        "STRAY CONFINED",
        "Bees/Wasp",
        "Dogs off Leash",
        "Stray - Confined",
        "Stray - At Large",
        "Injured - Domestic",
        "Investigate - Animal Care",
        "Cadaver - Domestic",
        "INJUR/DIST DOMESTIC",
        "ENF/INVEST ANIM CARE",
        "ENF/INVEST NO LEASH",
        "Investigate - Animal to Human Bite",
        "Dead Animal On Expressway",
        "Investigate - Menace",
        "Investigate - No Leash",
        "ENF/INVEST AN TO HU",
        "ENF/INVEST AN TO AN",
        "ENF/INVEST DAL HOME",
    },
    "Is_Noise": {
        "Noise",
        "Amplified Sound",
        "Construction Noise",
        "ENF/INVEST NOISE",
        "Unreasonable and Persistent Noise",
        "Investigate - Noise",
        "Amplified Sound or Instrument Sound",
        "Amplified or Musical Instrument Noise",
        "Stationary Source Noise",
        "Moving Motor Vehicle Noise",
        "Animal Noise",
        "Motor Vehicle Noise",
        "Power Device Noise",
        "Fireworks",
        "Stationary Source and Residential Air Conditioner Noise",
        "Loading and Unloading Noise",
        "Permitted or Exempted Noise",
        "Stationary Motor Vehicle Noise",
        "Noise Complaint",
    },
    "Is_Parking": {
        "Illegal Off-Street Parking",
        "Complaint/Investigation -Illegal Parking",
        "General Parking Regulations",
        "Litter / Bike Removal Inquiry",
    },
    "Is_Admin": {
        "Operator / Operations Complaint",
        "Dispute SR Status/Collections Curb Day",
        "Wrong Location/Time/Day",
        "Staff Conduct/Collections Curb Day",
        "Complaint-Outcome of the Service",
        "Complaint regarding Contractor",
        "Staff Service Complaint",
        "Complaint-Time Line of the Service",
        "Operational Comment or Complaint",
        "District Operations-Timeliness",
        "District Operations-Process",
        "Complaint-Process and Procedures",
        "Dispute SR Status/Collections FEL",
        "Complaint - Staff / Equipment / Attitude / Behaviour",
        "Operator / Operations Compliment",
        "Public Spaces Complaint",
        "Unknown - MLSBLEMMVN",
        "District Operations-Restoration",
        "Outcome of Service - Complaint - Road Operations",
        "District Operations-Contractor Related",
        "Comment / Suggestion",
        "Staff Service Compliment",
        "Complaint-Staff Conduct",
        "Multiple SRs/Collections Curb Day",
        "Compliment-Employee/Operation",
        "SERVICES PROT CUST",
        "Business Complaint",
        "Contractor Complaint",
    },
}

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
        temp_df = pd.read_csv(file, encoding="latin1", engine="python", usecols=range(9))
        all_dfs.append(temp_df.iloc[:, :9])

    df = pd.concat(all_dfs, ignore_index=True)
    df = df.iloc[:, :9]
    df.columns = [
        "Creation Date", "Status", "First 3 Chars of Postal Code",
        "Intersection Street 1", "Intersection Street 2", "Ward",
        "Service Request Type", "Division", "Section"
    ]
    df["Creation Date"] = pd.to_datetime(df["Creation Date"], errors="coerce")
    df = df.dropna(subset=["Creation Date", "Service Request Type"])
    return df



# =====================================================
# MAIN EXECUTION
# =====================================================

if __name__ == "__main__":

    df = load_ckan_data()
    total = len(df)

    # Apply exact rules
    for col, names in EXACT_RULES.items():
        df[col] = df["Service Request Type"].isin(names)

    df["Is_Other"] = ~df[list(EXACT_RULES.keys())].any(axis=1)

    # ---- Capture report ----
    print(f"\n{'='*55}")
    print(f"EXACT RULE CAPTURE REPORT  (n={total:,})")
    print(f"{'='*55}")
    print(f"{'Category':<22} {'Captured':>10} {'% of Total':>12}")
    print(f"{'-'*46}")

    any_matched = pd.Series(False, index=df.index)
    for col in EXACT_RULES.keys():
        count = df[col].sum()
        print(f"{col:<22} {count:>10,} {count/total*100:>11.2f}%")
        any_matched |= df[col]

    total_captured = any_matched.sum()
    print(f"{'-'*46}")
    print(f"{'TOTAL CAPTURED':<22} {total_captured:>10,} {total_captured/total*100:>11.2f}%")
    print(f"{'Is_Other (uncaptured)':<22} {df['Is_Other'].sum():>10,} {df['Is_Other'].mean()*100:>11.2f}%")
    print(f"{'='*55}\n")

    # ---- Uncaptured types — save to CSV for next iteration ----
    uncaptured = (
        df[df["Is_Other"]]["Service Request Type"]
        .value_counts()
        .reset_index()
    )
    uncaptured.columns = ["Service Request Type", "Count"]
    uncaptured.to_csv("uncaptured_service_types.csv", index=False)

    print("Top 20 uncaptured service types:")
    print(uncaptured.head(20).to_string(index=False))
    print(f"\nFull uncaptured list saved to uncaptured_service_types.csv")