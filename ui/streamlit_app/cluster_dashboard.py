import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from collections import Counter
import re


@st.cache_data
def load_csv(path_or_buffer):
    try:
        return pd.read_csv(path_or_buffer)
    except Exception as e:
        st.error(f"Error reading CSV: {e}")
        return pd.DataFrame()


def safe_path_for(filename: str) -> str:
    # project root assumed two levels up from this file
    root = Path(__file__).resolve().parents[2]
    p = root / filename
    return str(p)


STOPWORDS = set(
    [
        "the",
        "and",
        "to",
        "a",
        "is",
        "it",
        "for",
        "of",
        "this",
        "i",
        "in",
        "that",
        "they",
        "was",
        "with",
        "my",
        "on",
        "not",
        "but",
        "have",
        "are",
        "very",
    ]
)


def top_words(texts, n=20):
    cnt = Counter()
    for t in texts.dropna().astype(str):
        tokens = re.findall(r"\w{2,}", t.lower())
        tokens = [w for w in tokens if w not in STOPWORDS and not w.isdigit()]
        cnt.update(tokens)
    return cnt.most_common(n)


def main():
    st.set_page_config(page_title="Cluster Dashboard", layout="wide")
    st.title("Clustered Reviews — Tools & Home Improvement")

    st.sidebar.header("Data source")
    source = st.sidebar.radio("Choose input", ("Workspace CSVs (default)", "Upload CSV"))

    reviews_df = pd.DataFrame()
    profiles_df = pd.DataFrame()

    if source == "Upload CSV":
        uploaded = st.sidebar.file_uploader("Upload reviews CSV", type=["csv"])
        if uploaded is not None:
            reviews_df = load_csv(uploaded)
    else:
        # try default files in repo root
        reviews_path = safe_path_for("Tools_and_Home_Improvement_clustered.csv")
        profiles_path = safe_path_for("cluster_profiles.csv")
        st.sidebar.write("Using workspace files if present:")
        st.sidebar.code(reviews_path)
        st.sidebar.code(profiles_path)
        reviews_df = load_csv(reviews_path)
        profiles_df = load_csv(profiles_path)

    if reviews_df.empty and profiles_df.empty:
        st.info("No data loaded yet — upload a CSV or put the files in the project root.")
        return

    # if profiles not loaded but exist as aggregated info in reviews, compute small summary
    if profiles_df.empty and not reviews_df.empty:
        # create simple cluster summary
        try:
            profiles_df = (
                reviews_df.groupby("cluster")
                .agg(size=("user_id", "count"), avg_rating=("rating", "mean"))
                .reset_index()
            )
        except Exception:
            profiles_df = pd.DataFrame()

    # Show top-level charts
    st.header("Cluster Overview")
    c1, c2 = st.columns((2, 1))

    if not profiles_df.empty:
        with c1:
            st.subheader("Cluster sizes")
            if "size" in profiles_df.columns:
                fig = px.bar(profiles_df.sort_values("size", ascending=False), x="cluster", y="size", color="cluster", labels={"size":"# reviews"})
            else:
                fig = px.bar(profiles_df, x="cluster", y="avg_rating", color="cluster", labels={"avg_rating":"avg rating"})
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.subheader("Cluster metrics")
            display_cols = [c for c in ["cluster", "size", "avg_rating", "avg_review_length", "verified_ratio", "avg_price"] if c in profiles_df.columns]
            st.dataframe(profiles_df[display_cols].sort_values("cluster"), height=360)
    else:
        st.warning("No cluster profiles found — showing info derived from the reviews CSV.")

    # Reviews exploration
    st.header("Explore Reviews")
    if reviews_df.empty:
        st.info("No reviews file loaded.")
        return

    # quick guard: ensure cluster and text columns exist
    if "cluster" not in reviews_df.columns:
        st.error("The reviews file must contain a `cluster` column.")
        return

    clusters = sorted(reviews_df["cluster"].dropna().unique())
    sel_cluster = st.selectbox("Select cluster", options=["All"] + list(map(str, clusters)))

    min_rating, max_rating = int(reviews_df["rating"].min() if "rating" in reviews_df.columns else 1), int(reviews_df["rating"].max() if "rating" in reviews_df.columns else 5)
    rating_range = st.slider("Rating range", min_value=min_rating, max_value=max_rating, value=(min_rating, max_rating))

    keyword = st.text_input("Filter text (keyword)")
    n_samples = st.number_input("Rows to show", min_value=5, max_value=500, value=25)

    mask = pd.Series(True, index=reviews_df.index)
    if sel_cluster != "All":
        mask &= reviews_df["cluster"].astype(str) == str(sel_cluster)
    if "rating" in reviews_df.columns:
        mask &= reviews_df["rating"].between(rating_range[0], rating_range[1])
    if keyword:
        mask &= reviews_df.apply(lambda row: keyword.lower() in str(row.get("text", "")).lower(), axis=1)

    filtered = reviews_df[mask]

    st.subheader(f"Filtered reviews — {len(filtered):,} matches")
    st.dataframe(filtered.head(n_samples)[[c for c in ["user_id", "rating", "cluster", "text"] if c in filtered.columns]])

    st.subheader("Rating distribution")
    if "rating" in filtered.columns:
        fig2 = px.histogram(filtered, x="rating", nbins=5, title="Ratings")
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Top words in displayed reviews")
    if "text" in filtered.columns and not filtered["text"].dropna().empty:
        words = top_words(filtered["text"].head(1000), n=30)
        if words:
            wdf = pd.DataFrame(words, columns=["word", "count"])
            fig3 = px.bar(wdf, x="word", y="count", title="Top words")
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.write("No words found after filtering.")
    else:
        st.write("No textual reviews available to compute words.")


if __name__ == "__main__":
    main()
