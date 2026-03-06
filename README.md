# Capstone PCA and Clustering Pipeline

This project transforms raw Toronto 311 CKAN data into engineered feature sets to perform categorical, temporal, and spatial analysis. By leveraging Principal Component Analysis (PCA) and K-Means Clustering, the pipeline identifies structural patterns in municipal service requests across Toronto’s unique FSAs and Wards.

## Pipeline Core Components
### 1. Categorical Classification
Service requests are processed through rule-based buckets to standardize high-volume data:

Waste & Sanitation | Roads & Transportation | Water & Sewer

Property & Bylaw | Trees & Environment | Animal Services

Noise | Other

### 2. Temporal Feature Engineering
The model extracts multi-dimensional time features to identify cyclical patterns:

Cycles: Hour of day, Day of week, Month, Year.

Indicators: Weekend vs. Weekday, Nighttime vs. Daytime.

### 3. Geographic Aggregation
Data is pivoted and normalized at two distinct administrative levels:

FSA Level: Postal prefix analysis for granular neighborhood trends.

Ward Level: Municipal administrative boundary analysis (2018+ Ward structures).

### 4. Dimensionality Reduction & Clustering
PCA: Reduces dimensionality while preserving maximum variance to visualize geographic complaint profiles.

Clustering: Uses silhouette scores to automatically determine and assign optimal groupings of similar urban areas.

## Project Structure

### Primary Scripts
* `pipeline/build_feature_tables.py`
* `pipeline/temporal_visualizations.py`
* `pipeline/pca_and_clustering.py`

### Overall File Structure
<pre>
capstone-pca-and-clustering/
├── data/
│   ├── SR2010.csv
│   ├── SR2011.csv
│   ├── SR2012.csv
│   ├── SR2013.csv
│   ├── SR2014.csv
│   ├── SR2015.csv
│   ├── SR2016.csv
│   ├── SR2017.csv
│   ├── SR2018.csv
│   ├── SR2019.csv
│   ├── SR2020.csv
│   ├── SR2021.csv
│   ├── SR2022.csv
│   ├── SR2023.csv
│   ├── SR2024.csv
│   ├── SR2025.csv
│   └── SR2026.csv
├── pipeline/
│   ├── build_feature_tables.py
│   ├── temporal_visualizations.py
│   └── pca_and_clustering.py
├── requirements.txt
└── README.md
</pre>

### Workflow Overview
1. **Download and extract** raw 311 data files.
2. **Place** them in the `data` folder, which you create according to File Structure above.
3. **Build** feature tables.
4. **Run** temporal & category visualizations.
5. **Run** PCA and clustering.

---

## 1. Download and Prepare the 311 Data

You must download the Toronto 311 datasets for each year from CKAN.

### For Each Year
* Download the CSV file.
* Extract the file if it is compressed.
* Place the extracted .csv file inside the data folder.

> **Requirements:** All CSV files must be placed directly inside the data folder. The script reads every .csv file inside data. Files must remain raw and unmodified.

---

## 2. Set Up Virtual Environment

From the project root folder:

**Create Environment**
<pre>python -m venv venv</pre>

**Activate Environment**
* **Windows:** <pre>venv\Scripts\activate</pre>
* **Mac or Linux:** <pre>source venv/bin/activate</pre>

**Install Dependencies**
<pre>pip install -r requirements.txt</pre>

---

## 3. Build Feature Tables

Run the following command:
<pre>python pipeline/build_feature_tables.py</pre>

**This script will:**
* Load all CSV files inside data.
* Perform cleaning and validation.
* Apply FSA filtering.
* Apply Ward 2018+ filtering.
* Engineer time and category features.
* Generate aggregated feature tables.
* Export `CKAN_Feature_Tables.xlsx`.

---

## 4. Run Temporal & Category Visualizations

After feature tables are created, run:
<pre>python pipeline/temporal_visualizations.py</pre>

**This script will:**
* Generate yearly request trends.
* Generate monthly seasonal patterns.
* Generate hour-of-day distributions.
* Generate weekday vs weekend comparisons.
* Produce category-level summaries over time.
* Export visualizations as image files.

---

## 5. Run PCA and Clustering

After feature tables are created, run:
<pre>python pipeline/pca_and_clustering.py</pre>

**This script will:**
* Perform PCA on FSA and Ward feature tables.
* Automatically determine optimal cluster count using silhouette score.
* Apply K-means clustering.
* Generate PCA cluster plots.
* Generate cluster category composition plots.
* Export PCA and clustering results.
