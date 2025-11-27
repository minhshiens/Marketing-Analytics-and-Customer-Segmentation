import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Marketing Analytics Dashboard", layout="wide")

st.title("Marketing Analytics & Customer Segmentation")

st.markdown(
	"This dashboard exposes pre-computed KMeans cluster summaries and example reviews.\n"
	"Use the `Clusters` page from the left (or the Pages selector) to explore cluster profiles and sample reviews."
)

st.caption("CSV files expected at repository root: `cluster_profiles.csv` and `Tools_and_Home_Improvement_clustered.csv`.")
