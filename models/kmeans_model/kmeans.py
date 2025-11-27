import time

from narwhals import col

start_time = time.perf_counter()

import shutil
import numpy as np
import pandas as pd
import os
import sys

# Fix for Windows: Set PYSPARK_PYTHON to use the current Python interpreter
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator

spark = SparkSession.builder \
    .appName("Amazon Review Segmentation") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
    .getOrCreate()

print("Spark version:", spark.version)

# Load reviews data
data = spark.read.json(r"c:\Users\Darby\Downloads\Tools_and_Home_Improvement_2.jsonl")

df = data

columns_to_keep = [
    "user_id", "parent_asin", "text", "title", "rating", 
    "verified_purchase", "helpful_vote", "price", 
    "rating_number", "average_rating", "main_category", "timestamp"
]

df = df.select([col for col in columns_to_keep if col in df.columns])
print("s1")

# Clean numeric columns - replace malformed values with null
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

# Step 2: Create binary indicator
df = df.withColumn("has_price_listed",
    F.when(F.col("price").rlike("^[0-9]+\\.?[0-9]*$"), 1).otherwise(0))

# Step 3: For non-null prices, create buckets
price_median = df.filter(F.col("price").isNotNull()) \
    .selectExpr("percentile_approx(CAST(price AS DOUBLE), 0.5) as median") \
    .collect()[0].median

df = df.withColumn("price_category",
    F.when(F.col("price").isNull(), "no_price")
    .when(F.col("price").cast("double") < price_median * 0.5, "low")
    .when(F.col("price").cast("double") < price_median * 1.5, "medium")
    .otherwise("high"))

# Step 4: One-hot encode
for cat in ["no_price", "low", "medium", "high"]:
    df = df.withColumn(f"price_cat_{cat}",
        F.when(F.col("price_category") == cat, 1).otherwise(0))

df = df \
    .withColumn("review_length", F.length(F.col("text"))) \
    .withColumn("review_word_count", F.size(F.split(F.col("text"), "\\s+"))) \
    .withColumn("title_length", F.length(F.col("title"))) \
    .withColumn("verified_purchase_int", F.col("verified_purchase").cast("int")) \
    .withColumn("log_upvote", F.log1p(F.col("helpful_vote"))) \
    .withColumn("log_price", F.log1p(F.col("price"))) \
    .withColumn("log_product_reviews", F.log1p(F.col("rating_number"))) \
    .withColumn("rating_deviation", F.abs(F.col("rating") - F.col("average_rating"))) \
    .withColumn("simple_sentiment",
        F.when(F.col("rating") >= 4, 1.0)
        .when(F.col("rating") == 3, 0.5)
        .otherwise(0.0)
    ) \
    .withColumn("is_extreme_rating",
        F.when(F.abs(F.col("rating") - F.col("average_rating")) >= 2, 1).otherwise(0)
    )

print("s2")
print("s3")
print("s6")

user_counts = df.groupBy("user_id").count()
df = df.join(user_counts.withColumnRenamed("count", "user_total_reviews"), "user_id", "left")

# Instead of row_number (expensive), use a simpler approximation
df = df.withColumn("user_review_number", 
    F.when(F.col("user_total_reviews").isNull(), 1).otherwise(F.col("user_total_reviews"))
)

print("s7")
print("s8")
print("s9")
print("s10")

# I. Category encoding (one-hot for top categories)
top_categories_df = df.groupBy("main_category").count() \
    .orderBy(F.desc("count")) \
    .limit(5)

# Create category columns using Spark operations instead of collect
for row in top_categories_df.toLocalIterator():
    category = row['main_category']
    if category is not None:
        col_name = f"cat_{category.replace(' ', '_').replace('&', 'and')[:20]}"
        df = df.withColumn(
            col_name,
            F.when(F.col("main_category") == category, 1).otherwise(0)
    )

print("\n=== STEP 5: Feature Selection ===")

# Define feature columns for K-Means
feature_cols = [
    "review_length",
    "review_word_count",
    "title_length",
    "verified_purchase_int",
    "log_upvote",
    "log_product_reviews",
    "log_price",
    "rating_deviation",
    "user_review_number",
    "is_extreme_rating",
    "has_price_listed",
    "price_cat_no_price",
    "price_cat_low",
    "price_cat_medium",
    "price_cat_high"
]

print("s11")

# Add category features
category_cols = [col for col in df.columns if col.startswith("cat_")]
feature_cols.extend(category_cols)

print("s12")

# Filter out rows with nulls in key features
df_features = df.select(
    ["user_id", "parent_asin", "text", "rating", "helpful_vote", 
     "verified_purchase", "price"] + feature_cols
).na.drop(subset=feature_cols)

df_features.persist()

print("s13")

print("\n=== STEP 6: Feature Scaling ===")

# Assemble features into vector
assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features_raw",
    handleInvalid="skip"
)

print("s14")

# Scale features (K-Means is sensitive to scale)
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=False
)

print("s15")

# Create pipeline for preprocessing
preprocessing_pipeline = Pipeline(stages=[assembler, scaler])

print("s16")

# Fit and transform
preprocessing_model = preprocessing_pipeline.fit(df_features)
df_scaled = preprocessing_model.transform(df_features)

print("s17")

# Cache for multiple K-Means runs
df_scaled = df_scaled.select(
    "user_id", "parent_asin", "text", "rating", 
    "helpful_vote", "verified_purchase", "review_length", "price", "features"
)

df_scaled.persist()
df_scaled.count()  # Force computation

# Unpersist previous dataframe
df_features.unpersist()

print("\n=== STEP 7: Finding Optimal K ===")

k = 4
evaluator = ClusteringEvaluator(
    predictionCol="prediction",
    featuresCol="features",
    metricName="silhouette"
)

kmeans = KMeans(
    k=k,
    seed=42,
    featuresCol="features",
    predictionCol="prediction",
    maxIter=20
)

model_final = kmeans.fit(df_scaled)
df_clustered = model_final.transform(df_scaled)

# Evaluate FIRST (while column is still named "prediction")
silhouette_final = evaluator.evaluate(df_clustered)
print(f"  Silhouette Score: {silhouette_final:.4f}")
print(f"  Training Cost: {model_final.summary.trainingCost:.2f}")

# THEN rename for your analysis
df_clustered = df_clustered.withColumnRenamed("prediction", "cluster")
df_clustered.persist()
df_clustered.count()

# Unpersist scaled data
df_scaled.unpersist()

print("\n=== STEP 9: Cluster Profiling ===")

cluster_sizes = df_clustered.groupBy("cluster").count().orderBy("cluster")
print("\nCluster Sizes:")
cluster_sizes.show()

cluster_profiles = df_clustered.groupBy("cluster").agg(
    F.count("*").alias("size"),
    F.avg("rating").alias("avg_rating"),
    F.avg("helpful_vote").alias("avg_upvote"),
    F.avg("review_length").alias("avg_review_length"),
    F.avg(F.col("verified_purchase").cast("int")).alias("verified_ratio"),
    F.avg("price").alias("avg_price"),
    F.stddev("rating").alias("std_rating")
).orderBy("cluster")

print("\nCluster Profiles:")
cluster_profiles.show(truncate=False)

cluster_profiles_pd = cluster_profiles.coalesce(1).toPandas()
cluster_profiles_pd.to_csv(r"c:\Users\Darby\Downloads\Marketing-Analytics-and-Customer-Segmentation\cluster_profiles.csv", index=False)

print("\n=== STEP 10: Sample Reviews per Cluster ===")

for cluster_id in range(k):
    print(f"\n{'='*80}")
    print(f"CLUSTER {cluster_id}")
    print('='*80)
    
    samples = df_clustered.filter(F.col("cluster") == cluster_id) \
        .select("rating", "text", "helpful_vote", "verified_purchase") \
        .limit(3)
    
    for i, row in enumerate(samples, 1):
        print(f"\nSample {i}:")
        print(f"  Rating: {row['rating']}")
        print(f"  Upvotes: {row['helpful_vote']}")
        print(f"  Verified: {row['verified_purchase']}")
        print(f"  Text: {row['text'] if row['text'] is not None else "[No text]"[:200]}...")

print("\n=== STEP 11: Cluster Interpretation ===")

cluster_interpretation = df_clustered.groupBy("cluster").agg(
    F.avg("rating").alias("avg_rating"),
    F.avg("review_length").alias("avg_length"),
    F.avg("helpful_vote").alias("avg_upvote"),
    F.mean(F.col("verified_purchase").cast("int")).alias("verified_pct") 
).orderBy("cluster")

cluster_interpretation = cluster_interpretation.withColumn(
    "cluster_name",
    F.when(
        (F.col("avg_rating") >= 4.5) & (F.col("avg_length") > 100),
        F.lit("Enthusiastic Detailed Reviewers")
    ).when(
        (F.col("avg_rating") <= 2.5) & (F.col("avg_length") > 100),
        F.lit("Critical Detailed Reviewers")
    ).when(
        F.col("avg_upvote") > 10,
        F.lit("Helpful/Influential Reviewers")
    ).when(
        (F.col("verified_pct") > 0.8) & (F.col("avg_length") < 50),
        F.lit("Quick Verified Buyers")
    ).when(
        F.col("avg_length") < 30,
        F.lit("Minimal Reviewers")
    ).otherwise(
        F.lit("Balanced Reviewers")
    )
)

print("\nCluster Interpretation:")
cluster_interpretation.select(
    "cluster",
    "cluster_name",
    F.round("avg_rating", 2).alias("avg_rating"),
    F.round("avg_length", 1).alias("avg_length"),
    F.round("avg_upvote", 2).alias("avg_upvote"),
    (F.col("verified_pct") * 100).alias("verified_pct")
).show(truncate=False)

print("\n=== STEP 12: Saving Results ===")

# Option 1: Save as CSV using Pandas (easier to open)
df_clustered.select("user_id", "parent_asin", "rating", "cluster", "text") \
    .toPandas() \
    .to_csv(r"c:\Users\Darby\Downloads\Marketing-Analytics-and-Customer-Segmentation\Tools_and_Home_Improvement_clustered.csv", index=False)
print("Saved clustered reviews to 'Tools_and_Home_Improvement_clustered.csv'")


spark.stop()
print("\nPipeline completed successfully!")

end_time = time.perf_counter()
print(f"\nExecution time: {end_time - start_time:.2f} seconds")
