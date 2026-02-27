# Capstone PCA and Clustering Pipeline

This project builds engineered feature tables from Toronto 311 CKAN data and performs PCA and clustering analysis at the FSA and Ward level.

---

## Project Structure

### Primary Scripts
* `pipeline/build_feature_tables.py`
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
│   └── pca_and_clustering.py
├── requirements.txt
└── README.md
</pre>

### Workflow Overview
1. **Download and extract** raw 311 data files.
2. **Place** them in the `data` folder.
3. **Build** feature tables.
4. **Run** PCA and clustering.

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
* Export `CKAN_Feature_Tables.xlsx`.

---

## 4. Run PCA and Clustering

After feature tables are created, run:
<pre>python pipeline/pca_and_clustering.py</pre>

**This script will:**
* Perform PCA on FSA and Ward tables.
* Apply clustering.
* Generate PCA plots.
* Export PCA and clustering results.
