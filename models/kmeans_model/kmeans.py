import time
import sys
import os
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

# =============================================================================
# CONFIGURATION
# =============================================================================

INPUT_PATH = r"c:\Users\Darby\Downloads\Tools_and_Home_Improvement_2.jsonl"
OUTPUT_DIR = Path(r"c:\Users\Darby\Downloads\Marketing-Analytics-and-Customer-Segmentation")
N_CLUSTERS = 4
TOP_N_CATEGORIES = 5
KMEANS_MAX_ITER = 20
RANDOM_SEED = 42

# =============================================================================
# PREFECT TASKS
# =============================================================================

@task(name="Initialize Spark Session", retries=2, retry_delay_seconds=10)
def create_spark_session():
    """Initialize Spark with optimized settings"""
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    spark = SparkSession.builder \
        .appName("Amazon Review Segmentation") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
        .getOrCreate()
    
    print(f"✓ Spark {spark.version} initialized")
    return spark


@task(name="Load Data", retries=3, retry_delay_seconds=5)
def load_and_filter_data(spark, path):
    """Load JSON data and select relevant columns"""
    print("\n[Loading data...]")
    
    data = spark.read.json(path)
    
    columns_to_keep = [
        "user_id", "parent_asin", "text", "title", "rating", 
        "verified_purchase", "helpful_vote", "price", 
        "rating_number", "average_rating", "main_category", "timestamp"
    ]
    
    df = data.select([col for col in columns_to_keep if col in data.columns])
    row_count = df.count()
    print(f"  Loaded {row_count:,} reviews with {len(df.columns)} columns")
    
    return df, row_count


@task(name="Clean Numeric Columns")
def clean_numeric_columns(df):
    """Fix malformed numeric values"""
    print("\n[Cleaning numeric columns...]")
    
    df = df \
        .withColumn("price", 
            F.when(F.col("price").rlike("^[0-9.]+$"), F.col("price").cast("double"))
            .otherwise(None)) \
        .withColumn("helpful_vote",
            F.when(F.col("helpful_vote").isNull(), 0)
            .otherwise(F.col("helpful_vote").cast("int"))) \
        .withColumn("rating_number",
            F.when(F.col("rating_number").rlike("^[0-9]+$"), F.col("rating_number").cast("int"))
            .otherwise(None)) \
        .withColumn("average_rating",
            F.when(F.col("average_rating").rlike("^[0-9.]+$"), F.col("average_rating").cast("double"))
            .otherwise(None))
    
    print("  ✓ Numeric columns cleaned")
    return df


@task(name="Engineer Price Features")
def engineer_price_features(df):
    """Create essential price features handling 27% missing values"""
    print("\n[Engineering price features...]")
    
    # Check missing rate
    total_count = df.count()
    missing_count = df.filter(F.col("price").isNull()).count()
    missing_pct = (missing_count / total_count) * 100
    print(f"  Price missing rate: {missing_pct:.1f}% ({missing_count:,} / {total_count:,})")
    
    # Get median for imputation
    price_median = df.filter(F.col("price").isNotNull()) \
        .selectExpr("percentile_approx(CAST(price AS DOUBLE), 0.5) as median") \
        .collect()[0].median
    
    print(f"  Price median: ${price_median:.2f}")
    
    # Simple approach: just 2 features
    # 1. Binary indicator
    df = df.withColumn("has_price",
        F.when(F.col("price").isNotNull(), 1).otherwise(0))
    
    # 2. Median-imputed log price
    df = df.withColumn("price_filled",
        F.when(F.col("price").isNull(), price_median).otherwise(F.col("price")))
    
    df = df.withColumn("log_price", F.log1p(F.col("price_filled")))
    
    print("  ✓ Price features: has_price (binary) + log_price (continuous)")
    
    return df


@task(name="Engineer Review Features")
def engineer_review_features(df):
    """Create essential review-based features"""
    print("\n[Engineering review features...]")
    
    df = df \
        .withColumn("word_count", F.size(F.split(F.col("text"), "\\s+"))) \
        .withColumn("verified_int", F.col("verified_purchase").cast("int")) \
        .withColumn("rating_diff", F.col("rating") - F.col("average_rating")) \
        .withColumn("is_extreme", 
            F.when((F.col("rating") == 5) | (F.col("rating") == 1), 1).otherwise(0))
    
    # Simple engagement metric
    df = df.withColumn("engagement",
        F.when(F.col("word_count") > 0, F.col("helpful_vote") / F.col("word_count"))
        .otherwise(0))
    
    print("  ✓ Review features: word_count, verified_int, rating_diff, is_extreme, engagement")
    return df


@task(name="Engineer User Features")
def engineer_user_features(df):
    """Create essential user-level features"""
    print("\n[Engineering user features...]")
    
    # Basic user stats only
    user_stats = df.groupBy("user_id").agg(
        F.count("*").alias("user_reviews"),
        F.avg("rating").alias("user_avg_rating")
    )
    
    df = df.join(user_stats, "user_id", "left")
    
    print("  ✓ User features: user_reviews, user_avg_rating")
    return df


@task(name="Engineer Category Features")
def engineer_category_features(df, top_n=TOP_N_CATEGORIES):
    """Create simple category popularity feature"""
    print(f"\n[Engineering category features...]")
    
    # Just category popularity - no joins
    category_counts = df.groupBy("main_category").count()
    max_count = category_counts.agg(F.max("count")).collect()[0][0]
    
    category_popularity = category_counts.withColumn(
        "category_pop",
        F.col("count") / max_count
    ).select("main_category", "category_pop")
    
    df = df.join(category_popularity, "main_category", "left")
    df = df.withColumn("category_pop", F.coalesce(F.col("category_pop"), F.lit(0.0)))
    
    print("  ✓ Category feature: category_pop (0-1 scale)")
    return df


@task(name="Prepare Features for Clustering")
def prepare_features_for_clustering(df):
    """Select MINIMAL feature set for K-Means to avoid OOM"""
    print("\n[Preparing features for clustering...]")
    
    # MINIMAL FEATURE SET - only 12 features
    feature_cols = [
        # Core review features (4)
        "rating",                  # The rating itself
        "word_count",              # Review length
        "helpful_vote",            # How helpful
        "engagement",              # Upvotes per word
        
        # Verification & behavior (2)
        "verified_int",            # Verified purchase
        "is_extreme",              # Extreme rating (1 or 5)
        
        # Rating context (2)
        "rating_diff",             # vs product average
        "average_rating",          # Product's average
        
        # User context (2)
        "user_reviews",            # How active is user
        "user_avg_rating",         # User's typical rating
        
        # Price (2)
        "has_price",               # Binary: has price
        "log_price",               # Log-transformed price
    ]
    
    print(f"  Total features: {len(feature_cols)} (reduced from 30+)")
    
    # Select and filter nulls
    df_features = df.select(
        ["user_id", "parent_asin", "text", "rating", "helpful_vote", 
         "verified_purchase", "price"] + feature_cols
    ).na.drop(subset=feature_cols)
    
    original_count = df.count()
    filtered_count = df_features.count()
    print(f"  Rows after null filtering: {filtered_count:,} ({filtered_count/original_count*100:.1f}%)")
    
    # Assemble and scale
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw",
        handleInvalid="skip"
    )
    
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    pipeline = Pipeline(stages=[assembler, scaler])
    model = pipeline.fit(df_features)
    df_scaled = model.transform(df_features)
    
    # Select final columns - DON'T cache yet (save memory)
    df_scaled = df_scaled.select(
        "user_id", "parent_asin", "text", "rating", 
        "helpful_vote", "verified_purchase", "word_count", "price", "features"
    )
    
    print("  ✓ Features assembled and scaled (not cached to save memory)")
    return df_scaled


@task(name="Train K-Means Model")
def train_kmeans_model(df_scaled, k=N_CLUSTERS, max_iter=KMEANS_MAX_ITER):
    """Train K-Means clustering model"""
    print(f"\n[Training K-Means (k={k})...]")
    
    # Cache only before training
    df_scaled.persist()
    df_scaled.count()
    
    kmeans = KMeans(
        k=k,
        seed=RANDOM_SEED,
        featuresCol="features",
        predictionCol="prediction",
        maxIter=30,
        initMode="k-means||"
    )
    
    model = kmeans.fit(df_scaled)
    df_clustered = model.transform(df_scaled)
    
    # Evaluate
    evaluator = ClusteringEvaluator(
        predictionCol="prediction",
        featuresCol="features",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean"
    )
    silhouette = evaluator.evaluate(df_clustered)
    training_cost = model.summary.trainingCost
    
    print(f"  Silhouette Score: {silhouette:.4f}")
    print(f"  Training Cost: {training_cost:.2f}")
    print(f"  Iterations: {model.summary.numIter}")
    
    # Rename and cache result
    df_clustered = df_clustered.withColumnRenamed("prediction", "cluster")
    df_clustered.persist()
    df_clustered.count()
    
    # Unpersist input immediately
    df_scaled.unpersist()
    
    return df_clustered, {"silhouette_score": silhouette, "training_cost": training_cost}


@task(name="Profile Clusters")
def profile_clusters(df_clustered, k=N_CLUSTERS):
    """Generate cluster profiles and statistics"""
    print("\n" + "="*80)
    print("CLUSTER ANALYSIS")
    print("="*80)
    
    # Aggregate statistics
    cluster_profiles = df_clustered.groupBy("cluster").agg(
        F.count("*").alias("size"),
        F.avg("rating").alias("avg_rating"),
        F.avg("helpful_vote").alias("avg_upvote"),
        F.avg("word_count").alias("avg_word_count"),
        F.avg(F.col("verified_purchase").cast("int")).alias("verified_ratio"),
        F.avg("price").alias("avg_price"),
        F.stddev("rating").alias("std_rating")
    ).orderBy("cluster")
    
    print("\nCluster Profiles:")
    cluster_profiles.show(truncate=False)
    
    # Assign interpretable names
    cluster_interpretation = df_clustered.groupBy("cluster").agg(
        F.avg("rating").alias("avg_rating"),
        F.avg("review_word_count").alias("avg_word_count"),
        F.avg("helpful_vote").alias("avg_upvote"),
        F.mean(F.col("verified_purchase").cast("int")).alias("verified_pct") 
    ).orderBy("cluster")
    
    cluster_interpretation = cluster_interpretation.withColumn("cluster_name",
        F.when((F.col("avg_rating") >= 4.5) & (F.col("avg_word_count") > 50),
            F.lit("Enthusiastic Detailed Reviewers"))
        .when((F.col("avg_rating") <= 2.5) & (F.col("avg_word_count") > 50),
            F.lit("Critical Detailed Reviewers"))
        .when(F.col("avg_upvote") > 10,
            F.lit("Helpful/Influential Reviewers"))
        .when((F.col("verified_pct") > 0.8) & (F.col("avg_word_count") < 30),
            F.lit("Quick Verified Buyers"))
        .when(F.col("avg_word_count") < 20,
            F.lit("Minimal Reviewers"))
        .otherwise(F.lit("Balanced Reviewers"))
    )
    
    print("\nCluster Interpretation:")
    cluster_interpretation.select(
        "cluster",
        "cluster_name",
        F.round("avg_rating", 2).alias("avg_rating"),
        F.round("avg_word_count", 1).alias("avg_words"),
        F.round("avg_upvote", 2).alias("avg_upvote"),
        (F.col("verified_pct") * 100).alias("verified_pct")
    ).show(truncate=False)
    
    # Sample reviews
    print("\nSample Reviews per Cluster:")
    for cluster_id in range(k):
        print(f"\n{'─'*80}")
        print(f"CLUSTER {cluster_id}")
        print('─'*80)
        
        samples = df_clustered.filter(F.col("cluster") == cluster_id) \
            .select("rating", "text", "helpful_vote", "verified_purchase") \
            .limit(3) \
            .collect()
        
        for i, row in enumerate(samples, 1):
            print(f"\nSample {i}:")
            print(f"  Rating: {row['rating']} | Upvotes: {row['helpful_vote']} | Verified: {row['verified_purchase']}")
            text = row['text'] if row['text'] else "[No text]"
            print(f"  Text: {text[:200]}{'...' if len(text) > 200 else ''}")
    
    return cluster_profiles


@task(name="Save Results")
def save_results(df_clustered, cluster_profiles, output_dir):
    """Save clustering results to CSV files"""
    print("\n" + "="*80)
    print("SAVING RESULTS")
    print("="*80)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save cluster profiles
    profiles_path = output_dir / "cluster_profiles.csv"
    cluster_profiles.coalesce(1).toPandas().to_csv(profiles_path, index=False)
    print(f"✓ Cluster profiles saved to: {profiles_path}")
    
    # Save clustered reviews
    clustered_path = output_dir / "Tools_and_Home_Improvement_clustered.csv"
    df_clustered.select("user_id", "parent_asin", "rating", "cluster", "text") \
        .toPandas() \
        .to_csv(clustered_path, index=False)
    print(f"✓ Clustered reviews saved to: {clustered_path}")
    
    return str(profiles_path), str(clustered_path)


@task(name="Cleanup Spark Session")
def cleanup_spark(spark):
    """Stop Spark session"""
    print("\n[Cleaning up Spark session...]")
    spark.stop()
    print("  ✓ Spark session stopped")


# =============================================================================
# PREFECT FLOW
# =============================================================================

@flow(name="Amazon Review Segmentation Pipeline", log_prints=True)
def review_segmentation_flow(
    input_path: str = INPUT_PATH,
    output_dir: Path = OUTPUT_DIR,
    n_clusters: int = N_CLUSTERS
):
    """
    Main Prefect flow for Amazon review clustering and segmentation.
    
    Args:
        input_path: Path to the input JSONL file
        output_dir: Directory to save output files
        n_clusters: Number of clusters for K-Means
    """
    start_time = time.perf_counter()
    
    print("\n" + "="*80)
    print("AMAZON REVIEW SEGMENTATION PIPELINE (Prefect)")
    print("="*80)
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Data loading and feature engineering pipeline
        df, row_count = load_and_filter_data(spark, input_path)
        df = clean_numeric_columns(df)
        df = engineer_price_features(df)
        df = engineer_review_features(df)
        df = engineer_user_features(df)
        df = engineer_category_features(df)
        
        # Clustering pipeline
        df_scaled = prepare_features_for_clustering(df)
        df_clustered, metrics = train_kmeans_model(df_scaled, k=n_clusters)
        
        # Analysis and saving
        cluster_profiles = profile_clusters(df_clustered, k=n_clusters)
        profiles_path, clustered_path = save_results(df_clustered, cluster_profiles, output_dir)
        
        # Cleanup
        df_clustered.unpersist()
        cleanup_spark(spark)
        
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        
        print("\n" + "="*80)
        print(f"✓ Pipeline completed in {execution_time:.2f} seconds")
        print("="*80)
        
        # Return summary for Prefect
        return {
            "execution_time_seconds": execution_time,
            "total_reviews": row_count,
            "n_clusters": n_clusters,
            "silhouette_score": metrics["silhouette_score"],
            "training_cost": metrics["training_cost"],
            "output_files": {
                "cluster_profiles": profiles_path,
                "clustered_reviews": clustered_path
            }
        }
        
    except Exception as e:
        cleanup_spark(spark)
        raise e


# =============================================================================
# MAIN ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    # Run the flow
    result = review_segmentation_flow()
    
    print("\n" + "="*80)
    print("PIPELINE SUMMARY")
    print("="*80)
    print(f"Execution Time: {result['execution_time_seconds']:.2f} seconds")
    print(f"Total Reviews Processed: {result['total_reviews']:,}")
    print(f"Number of Clusters: {result['n_clusters']}")
    print(f"Silhouette Score: {result['silhouette_score']:.4f}")
    print(f"Training Cost: {result['training_cost']:.2f}")
    print(f"\nOutput Files:")
    print(f"  - Cluster Profiles: {result['output_files']['cluster_profiles']}")
    print(f"  - Clustered Reviews: {result['output_files']['clustered_reviews']}")