
import streamlit as st
import pandas as pd
from pathlib import Path


def repo_root() -> Path:
	# file is in ui/streamlit_app/pages -> go up 3 levels to repo root
	return Path(__file__).resolve().parents[3]


@st.cache_data(show_spinner=False)
def load_cluster_profiles(path: Path) -> pd.DataFrame:
	return pd.read_csv(path)


@st.cache_data(show_spinner=False)
def load_clustered_reviews(path: Path) -> pd.DataFrame:
	# CSV may be large; load only required columns if possible
	return pd.read_csv(path)


def show():
	st.title("Clusters: profiles & sample reviews")

	root = repo_root()
	profiles_path = root / "cluster_profiles.csv"
	reviews_path = root / "Tools_and_Home_Improvement_clustered.csv"

	if not profiles_path.exists():
		st.error(f"`cluster_profiles.csv` not found at {profiles_path}")
		return

	profiles = load_cluster_profiles(profiles_path)

	st.header("Cluster Profiles")
	st.dataframe(profiles)

	st.subheader("Cluster sizes")
	sizes = profiles.set_index('cluster')['size']
	st.bar_chart(sizes)

	st.markdown("---")

	if not reviews_path.exists():
		st.warning(f"Clustered reviews CSV not found at {reviews_path}. You can still view profiles above.")
		return

	reviews = load_clustered_reviews(reviews_path)

	st.header("Browse sample reviews by cluster")
	cluster_options = sorted(reviews['cluster'].dropna().unique().tolist())
	selected = st.selectbox("Select cluster", cluster_options, index=0)

	filtered = reviews[reviews['cluster'] == selected]
	st.metric("Reviews in cluster", f"{len(filtered):,}")

	# show aggregate metrics if present
	cols_to_show = []
	if 'rating' in filtered.columns:
		cols_to_show.append('rating')
	if 'avg_rating' in profiles.columns:
		profile_avg = profiles.loc[profiles['cluster'] == selected, 'avg_rating']
		if not profile_avg.empty:
			st.write(f"Profile average rating: **{profile_avg.iloc[0]:.2f}**")

	if cols_to_show:
		st.subheader("Rating distribution")
		st.bar_chart(filtered[cols_to_show].value_counts().sort_index())

	st.subheader("Sample reviews")
	# show a few sample texts with user and rating
	sample_n = st.slider("Number of sample reviews to show", min_value=5, max_value=50, value=10)
	display_cols = []
	for c in ['user_id', 'parent_asin', 'rating', 'text']:
		if c in filtered.columns:
			display_cols.append(c)

	if display_cols:
		st.dataframe(filtered[display_cols].head(sample_n))
	else:
		st.write(filtered.head(sample_n))

	# allow download of filtered CSV
	csv = filtered.to_csv(index=False).encode('utf-8')
	st.download_button("Download filtered CSV", csv, file_name=f"cluster_{selected}_reviews.csv", mime='text/csv')


if __name__ == '__main__':
	show()

