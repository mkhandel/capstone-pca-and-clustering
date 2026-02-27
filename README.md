Capstone PCA and Clustering Pipeline

This project builds engineered feature tables from Toronto 311 CKAN data and performs PCA and clustering analysis at the FSA and Ward level.

The pipeline consists of two main scripts:

pipeline/build_feature_tables.py
pipeline/pca_and_clustering.py

The workflow is:

Download and extract raw 311 data files

Place them in the data folder

Build feature tables

Run PCA and clustering

1. Download and Prepare the 311 Data

You must download the Toronto 311 datasets for each year from CKAN.

For each year:

Download the CSV file

Extract the file if it is compressed

Place the extracted .csv file inside the data folder

Important:

All CSV files must be placed directly inside the data folder

The script reads every .csv file inside data

Example folder structure:

capstone-pca-and-clustering/
│
├── data/
│   ├── SR2018.csv
│   ├── SR2019.csv
│   ├── SR2020.csv
│   ├── SR2021.csv
│   ├── SR2022.csv
│   ├── SR2023.csv
│   ├── SR2024.csv
│   ├── SR2025.csv
│   └── SR2026.csv

All files must remain raw and unmodified.

2. Set Up Virtual Environment

From the project root folder:

Create virtual environment:

python -m venv venv

Activate virtual environment.

On Windows:

venv\Scripts\activate

On Mac or Linux:

source venv/bin/activate

Install dependencies:

pip install -r requirements.txt
3. Build Feature Tables

Run:

python pipeline/build_feature_tables.py

This will:

Load all CSV files inside data

Perform cleaning and validation

Apply FSA filtering

Apply Ward 2018+ filtering

Engineer time and category features

Export CKAN_Feature_Tables.xlsx

The Excel file will contain:

FSA sheet

Ward sheet

Service_Type sheet

4. Run PCA and Clustering

After feature tables are created, run:

python pipeline/pca_and_clustering.py

This will:

Perform PCA on FSA and Ward tables

Apply clustering

Generate PCA plots

Export PCA and clustering results

Output files include:

PCA_Results.xlsx

Clustering_Results.xlsx

FSA_pca_clusters.png

Ward_pca_clusters.png

Notes

The data folder is ignored by Git and must be created locally

Generated Excel and PNG files are ignored by Git

Ward analysis only includes data from 2018 and later due to ward restructuring

FSA analysis includes all available years minus the entries with intersection included. 

