import pandas as pd
import numpy as np
import matplotlib.pyplot as plt #added for visualization
from pathlib import Path

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score



# =========================================
# SETTINGS
# =========================================
ENGINEERED_PATH = Path("CKAN_Feature_Tables.xlsx")
PCA_OUTPUT = Path("PCA_Results.xlsx")
CLUSTER_OUTPUT = Path("Clustering_Results.xlsx")

RANDOM_STATE = 42
VARIANCE_TARGET = 0.90


# =========================================
# Helper Functions
# =========================================

def pick_k_by_silhouette(X, k_min=2, k_max=10):
    n = X.shape[0]

    if n < 3:
        return None, None, None

    k_max = min(k_max, n - 1)

    best_k = None
    best_score = -1
    scores = []

    for k in range(k_min, k_max + 1):
        kmeans = KMeans(n_clusters=k, n_init=20, random_state=RANDOM_STATE)
        labels = kmeans.fit_predict(X)

        if len(set(labels)) < 2:
            scores.append((k, np.nan))
            continue

        score = silhouette_score(X, labels)
        scores.append((k, score))

        if score > best_score:
            best_score = score
            best_k = k

    scores_df = pd.DataFrame(scores, columns=["k", "silhouette"])
    return best_k, best_score, scores_df


def run_pca(X):
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Fit full PCA to determine # components
    pca_full = PCA(random_state=RANDOM_STATE)
    pca_full.fit(X_scaled)

    cumulative_variance = np.cumsum(pca_full.explained_variance_ratio_)
    n_components = np.searchsorted(cumulative_variance, VARIANCE_TARGET) + 1
    n_components = max(2, min(n_components, X.shape[1]))

    pca = PCA(n_components=n_components, random_state=RANDOM_STATE)
    scores = pca.fit_transform(X_scaled)

    scores_df = pd.DataFrame(
        scores,
        columns=[f"PC{i+1}" for i in range(n_components)]
    )

    explained_df = pd.DataFrame({
        "PC": [f"PC{i+1}" for i in range(n_components)],
        "explained_variance_ratio": pca.explained_variance_ratio_,
        "cumulative_variance": np.cumsum(pca.explained_variance_ratio_)
    })

    loadings_df = pd.DataFrame(
        pca.components_.T,
        index=X.columns,
        columns=[f"PC{i+1}" for i in range(n_components)]
    ).reset_index().rename(columns={"index": "feature"})

    return scores_df, explained_df, loadings_df


# =========================================
# Load Engineered Tables
# =========================================

xls = pd.ExcelFile(ENGINEERED_PATH)

#Updated to work with both ward or postal 

tables = {}

# Detect available geo sheet dynamically
available_sheets = xls.sheet_names

if "FSA" in available_sheets:
    tables["FSA"] = ("FSA", ["First 3 Chars of Postal Code"])

if "Ward" in available_sheets:
    tables["Ward"] = ("Ward", ["Ward"])

# Always include non-geo tables
tables.update({
    "Hour": ("Hour", ["Hour"]),
    "Day_of_Week": ("Day_of_Week", ["Day_of_Week"]),
    "Month": ("Month", ["Month"]),
    "Service_Type": ("Service_Type", ["Service Request Type"]),
})

summary_rows = []

with pd.ExcelWriter(PCA_OUTPUT, engine="xlsxwriter") as pca_writer, \
     pd.ExcelWriter(CLUSTER_OUTPUT, engine="xlsxwriter") as cluster_writer:

    for table_name, (sheet_name, id_cols) in tables.items():

        print(f"Processing {table_name}...")

        df = pd.read_excel(xls, sheet_name=sheet_name)

        X = df.drop(columns=id_cols).select_dtypes(include=[np.number])

        if X.shape[0] < 3 or X.shape[1] < 2:
            summary_rows.append({
                "table": table_name,
                "status": "Skipped (insufficient data)"
            })
            continue

        # ======================
        # PCA
        # ======================
        scores_df, explained_df, loadings_df = run_pca(X)

        pd.concat([df[id_cols], scores_df], axis=1) \
            .to_excel(pca_writer, sheet_name=f"{table_name}_scores", index=False)

        explained_df.to_excel(pca_writer, sheet_name=f"{table_name}_explained", index=False)
        loadings_df.to_excel(pca_writer, sheet_name=f"{table_name}_loadings", index=False)

        # ======================
        # Clustering
        # ======================
        best_k, best_silhouette, silhouette_curve = pick_k_by_silhouette(scores_df.values)

        if best_k is None:
            summary_rows.append({
                "table": table_name,
                "status": "PCA ok, clustering skipped"
            })
            continue

        kmeans = KMeans(n_clusters=best_k, n_init=30, random_state=RANDOM_STATE)
        cluster_labels = kmeans.fit_predict(scores_df.values)

        clustered_df = pd.concat(
            [df[id_cols], scores_df, pd.Series(cluster_labels, name="cluster")],
            axis=1
        )

        clustered_df.to_excel(cluster_writer, sheet_name=f"{table_name}_labels", index=False)

        # ======================
        # Simple visualization (geo only)
        # ======================
        if table_name in ["FSA", "Ward"]:

            plt.figure(figsize=(10, 8))

            if "PC1" in clustered_df.columns and "PC2" in clustered_df.columns:

                # Plot clusters
                for c in sorted(clustered_df["cluster"].unique()):
                    sub = clustered_df[clustered_df["cluster"] == c]

                    plt.scatter(
                        sub["PC1"],
                        sub["PC2"],
                        label=f"Cluster {c}",
                        alpha=0.75
                    )

                # Label ALL geo units (FSA or Ward)
                for _, row in clustered_df.iterrows():

                    label_value = row[id_cols[0]]

                    if pd.isna(label_value):
                        continue

                    label_value = str(label_value).strip()

                    plt.text(
                        row["PC1"],
                        row["PC2"],
                        label_value,
                        fontsize=7,
                        ha="center",
                        va="center",
                        alpha=0.9
                    )

                plt.xlabel("PC1")
                plt.ylabel("PC2")
                plt.title(f"{table_name}: PCA (PC1 vs PC2)")
                plt.legend()
                plt.grid(True)

                out_png = Path(f"{table_name}_pca_clusters.png")
                plt.savefig(out_png, dpi=200, bbox_inches="tight")
                print(f"Saved plot: {out_png}")

                plt.close()
                

       

        centers_df = pd.DataFrame(
            kmeans.cluster_centers_,
            columns=scores_df.columns
        )
        centers_df.insert(0, "cluster", range(best_k))
        centers_df.to_excel(cluster_writer, sheet_name=f"{table_name}_centers", index=False)

        silhouette_curve.to_excel(cluster_writer, sheet_name=f"{table_name}_silhouette", index=False)

        summary_rows.append({
            "table": table_name,
            "status": "OK",
            "rows": X.shape[0],
            "features": X.shape[1],
            "pca_components": scores_df.shape[1],
            "optimal_k": best_k,
            "silhouette_score": best_silhouette
        })

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_excel(pca_writer, sheet_name="SUMMARY", index=False)
    summary_df.to_excel(cluster_writer, sheet_name="SUMMARY", index=False)

print("PCA and clustering analysis complete.")
print(f"Saved to: {PCA_OUTPUT} and {CLUSTER_OUTPUT}")