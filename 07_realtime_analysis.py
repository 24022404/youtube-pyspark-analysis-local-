"""
07_realtime_analysis.py
Real-time YouTube Analytics - IMPROVED PATTERN-BASED ML
"""

import os
import sys
from datetime import datetime
from kafka import KafkaConsumer
import json
import pandas as pd
import numpy as np
from pymongo import MongoClient
import warnings
warnings.filterwarnings('ignore')

# Setup Java
os.environ['JAVA_HOME'] = 'C:\\Java\\jdk-1.8'
os.environ['HADOOP_HOME'] = os.environ.get('JAVA_HOME')
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}\\bin;{os.environ.get('PATH', '')}"

import findspark
findspark.init()

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import tempfile

print("=" * 70)
print("YOUTUBE REAL-TIME ANALYSIS - IMPROVED PATTERN MODEL")
print("=" * 70)

# =====================================================================
# 1. SPARK SESSION
# =====================================================================
print("\n[1/4] Initializing Spark Session...")

spark = SparkSession.builder \
    .appName("YouTubeRealTimeAnalysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.warehouse.dir", tempfile.gettempdir()) \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(f"✅ Spark {spark.version} started")

# =====================================================================
# 2. TRAIN IMPROVED PATTERN-BASED MODEL
# =====================================================================
print("\n[2/4] Training IMPROVED Pattern-based Model...")

# Load data
train_df = spark.read.csv("./data/preprocessed_data.csv", header=True, inferSchema=True)
print(f"   Loaded {train_df.count():,} training samples")

# Sample 15% (tăng từ 10% để có nhiều pattern hơn)
train_df = train_df.sample(fraction=0.15, seed=42)
print(f"   Sampled {train_df.count():,} samples")

# Convert to numeric
train_df = train_df.withColumn('view_count', col('view_count').cast('double')) \
    .withColumn('likes', col('likes').cast('double')) \
    .withColumn('comment_count', col('comment_count').cast('double')) \
    .withColumn('categoryId', col('categoryId').cast('int'))

# ========== FILTER QUALITY DATA ==========
# Lọc bỏ outlier và data không chất lượng
print("   Filtering quality data...")

train_df = train_df.filter(
    (col('view_count') >= 1000) &  # Bỏ video quá nhỏ
    (col('view_count') <= 50000000) &  # Bỏ outlier quá lớn
    (col('likes') > 0) &  # Phải có likes
    (col('comment_count') >= 0)
)

print(f"   After filtering: {train_df.count():,} quality samples")

# ========== ENHANCED PATTERN FEATURES ==========
print("   Creating ENHANCED pattern features...")

# 1. Engagement Rate (likes + comments / views)
train_df = train_df.withColumn(
    'engagement_rate',
    (col('likes') + col('comment_count')) / (col('view_count') + 1) * 100
)

# 2. Like Rate
train_df = train_df.withColumn(
    'like_rate',
    col('likes') / (col('view_count') + 1) * 100
)

# 3. Comment Rate
train_df = train_df.withColumn(
    'comment_rate',
    col('comment_count') / (col('view_count') + 1) * 100
)

# 4. Like to Comment Ratio (interaction quality)
train_df = train_df.withColumn(
    'like_comment_ratio',
    col('likes') / (col('comment_count') + 1)
)

# 5. View Scale (log normalization)
train_df = train_df.withColumn(
    'view_scale',
    log10(col('view_count') + 1)
)

# 6. Engagement Scale (log của total engagement)
train_df = train_df.withColumn(
    'engagement_scale',
    log10(col('likes') + col('comment_count') + 1)
)

# 7. Publish Hour
train_df = train_df.withColumn(
    'publish_hour',
    hour(col('publishedAt'))
)

# 8. Day of Week
train_df = train_df.withColumn(
    'day_of_week',
    dayofweek(col('publishedAt'))
)

# 9. Is Weekend (pattern khác biệt)
train_df = train_df.withColumn(
    'is_weekend',
    when((col('day_of_week') == 1) | (col('day_of_week') == 7), 1.0).otherwise(0.0)
)

# 10. Is Prime Time (6PM - 11PM)
train_df = train_df.withColumn(
    'is_prime_time',
    when((col('publish_hour') >= 18) & (col('publish_hour') <= 23), 1.0).otherwise(0.0)
)

# ========== TARGET: GROWTH POTENTIAL (thay vì số tuyệt đối) ==========
# Dự đoán "potential growth rate" thay vì "exact views"
train_df = train_df.withColumn(
    'growth_potential',
    log10(col('view_count') + 1) * (1 + col('engagement_rate') / 100)
)

# ========== FEATURES & LABEL ==========
feature_cols = [
    'categoryId',
    'engagement_rate',
    'like_rate',
    'comment_rate',
    'like_comment_ratio',
    'view_scale',
    'engagement_scale',
    'publish_hour',
    'day_of_week',
    'is_weekend',
    'is_prime_time'
]

label_col = 'growth_potential'

print(f"   Features: {len(feature_cols)} enhanced patterns")
print(f"   Label: {label_col} (growth potential)")

# Clean data
train_clean = train_df.select(feature_cols + [label_col, 'view_count']).na.drop()
print(f"   Clean samples: {train_clean.count():,}")

# ========== TIME-BASED SPLIT ==========
print("   Time-based train/validation split...")

train_sorted = train_df.withColumn('pub_timestamp', unix_timestamp(col('publishedAt'))) \
    .orderBy('pub_timestamp')

train_sorted = train_sorted.withColumn('row_num', row_number().over(Window.orderBy('pub_timestamp')))
total_rows = train_sorted.count()
split_point = int(total_rows * 0.8)

train_set = train_sorted.filter(col('row_num') <= split_point) \
    .select(feature_cols + [label_col]).na.drop()

valid_set = train_sorted.filter(col('row_num') > split_point) \
    .select(feature_cols + [label_col]).na.drop()

print(f"   Train: {train_set.count():,} | Valid: {valid_set.count():,}")

# ========== TRAIN IMPROVED MODEL ==========
print("   Training Gradient Boosted Trees (better than RF)...")

assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")

# StandardScaler để normalize features
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withStd=True,
    withMean=True
)

# GBT (tốt hơn Random Forest cho pattern learning)
gbt = GBTRegressor(
    featuresCol="features",
    labelCol=label_col,
    predictionCol="prediction",
    maxIter=50,  # Nhiều iterations hơn
    maxDepth=6,
    stepSize=0.1,
    seed=42
)

pipeline = Pipeline(stages=[assembler, scaler, gbt])
model = pipeline.fit(train_set)

print("   ✅ Model trained!")

# ========== VALIDATE ==========
print("   Validating...")

predictions = model.transform(valid_set)

rmse_eval = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
mae_eval = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="mae")
r2_eval = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="r2")

rmse = rmse_eval.evaluate(predictions)
mae = mae_eval.evaluate(predictions)
r2 = r2_eval.evaluate(predictions)

# MAPE
pred_pd = predictions.select(label_col, "prediction").toPandas()
actual = pred_pd[label_col]
predicted = pred_pd['prediction']
mape = (np.abs(actual - predicted) / (np.abs(actual) + 1e-10)).mean() * 100

print(f"   📊 RMSE: {rmse:.4f} | MAE: {mae:.4f} | R²: {r2:.4f}")
print(f"   📊 MAPE: {mape:.2f}%")

if mape < 20:
    print("   ✅ Model quality: EXCELLENT (MAPE < 20%)")
elif mape < 35:
    print("   ✅ Model quality: GOOD (MAPE < 35%)")
elif mape < 50:
    print("   ⚠️  Model quality: ACCEPTABLE (MAPE < 50%)")
else:
    print("   ⚠️  Model quality: NEEDS IMPROVEMENT (MAPE > 50%)")

print("✅ Model ready for predictions")

# =====================================================================
# 3. MONGODB
# =====================================================================
print("\n[3/4] Connecting to MongoDB...")

try:
    mongo_client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
    mongo_client.server_info()
    
    db = mongo_client['youtube_analytics']
    realtime_col = db['realtime_data']
    category_col = db['category_stats']
    predictions_col = db['predictions']
    
    print("✅ MongoDB connected")
except Exception as e:
    print(f"⚠️  MongoDB not available: {e}")
    mongo_client = None

# =====================================================================
# 4. KAFKA CONSUMER
# =====================================================================
print("\n[4/4] Connecting to Kafka...")

consumer = KafkaConsumer(
    'youtube-trending',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='youtube-analytics-group'
)

print("✅ Kafka connected")
print("\n" + "=" * 70)
print("🚀 REAL-TIME PROCESSING STARTED")
print("=" * 70)
print("\nListening to Kafka topic: youtube-trending")
print("Processing batch every 10 messages...\n")

# =====================================================================
# 5. REAL-TIME PROCESSING
# =====================================================================
batch = []
batch_count = 0
BATCH_SIZE = 10

try:
    for message in consumer:
        data = message.value
        batch.append(data)
        
        if len(batch) >= BATCH_SIZE:
            batch_count += 1
            
            print(f"\n{'='*70}")
            print(f"BATCH {batch_count} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*70}")
            
            df_pd = pd.DataFrame(batch)
            
            # ============ ANALYSIS ============
            print(f"\n📊 REAL-TIME ANALYSIS:")
            print(f"   Videos: {len(df_pd)}")
            print(f"   Total views: {df_pd['view_count'].sum():,}")
            
            # Calculate patterns
            df_pd['engagement_rate'] = ((df_pd['likes'] + df_pd['comment_count']) / (df_pd['view_count'] + 1) * 100)
            df_pd['like_rate'] = (df_pd['likes'] / (df_pd['view_count'] + 1) * 100)
            df_pd['comment_rate'] = (df_pd['comment_count'] / (df_pd['view_count'] + 1) * 100)
            df_pd['like_comment_ratio'] = df_pd['likes'] / (df_pd['comment_count'] + 1)
            df_pd['view_scale'] = np.log10(df_pd['view_count'] + 1)
            df_pd['engagement_scale'] = np.log10(df_pd['likes'] + df_pd['comment_count'] + 1)
            
            print(f"   Avg engagement: {df_pd['engagement_rate'].mean():.2f}%")
            
            # Category stats
            category_stats = df_pd.groupby('categoryId').agg({
                'view_count': ['count', 'sum', 'mean'],
                'engagement_rate': 'mean'
            }).round(2)
            
            print(f"\n📂 CATEGORY DISTRIBUTION:")
            print(category_stats)
            
            # Top videos
            top_videos = df_pd.nlargest(3, 'view_count')[['title', 'view_count', 'channelTitle']]
            print(f"\n🔥 TOP 3 VIDEOS:")
            for i, row in enumerate(top_videos.itertuples(), 1):
                print(f"   {i}. {row.title[:50]}... | {row.view_count:,} views")
            
            # ============ PREDICTIONS ============
            try:
                from pyspark.ml.linalg import Vectors
                
                # Extract time features
                df_pd['publishedAt_dt'] = pd.to_datetime(df_pd['publishedAt'])
                df_pd['publish_hour'] = df_pd['publishedAt_dt'].dt.hour
                df_pd['day_of_week'] = df_pd['publishedAt_dt'].dt.dayofweek + 1
                df_pd['is_weekend'] = df_pd['day_of_week'].isin([1, 7]).astype(float)
                df_pd['is_prime_time'] = df_pd['publish_hour'].between(18, 23).astype(float)
                
                predictions_list = []
                for idx, row in df_pd.iterrows():
                    # 11 features
                    features = Vectors.dense([
                        float(row['categoryId']),
                        float(row['engagement_rate']),
                        float(row['like_rate']),
                        float(row['comment_rate']),
                        float(row['like_comment_ratio']),
                        float(row['view_scale']),
                        float(row['engagement_scale']),
                        float(row['publish_hour']),
                        float(row['day_of_week']),
                        float(row['is_weekend']),
                        float(row['is_prime_time'])
                    ])
                    
                    single_row = spark.createDataFrame([(features,)], ["raw_features"])
                    
                    # Predict growth_potential
                    pred_growth = model.stages[-1].transform(
                        model.stages[1].transform(single_row)
                    ).select("prediction").first()[0]
                    
                    # Convert growth_potential back to views
                    # growth_potential = log10(views) * (1 + engagement_rate/100)
                    # Approximate: views ≈ 10^(growth_potential / (1 + engagement_rate/100))
                    estimated_multiplier = 1 + row['engagement_rate'] / 100
                    pred_log_views = pred_growth / estimated_multiplier
                    pred_views = 10 ** pred_log_views
                    
                    predictions_list.append(pred_views)
                
                df_pd['predicted_views_tomorrow'] = predictions_list
                
                print(f"\n🔮 PREDICTIONS (Pattern-based):")
                print(f"   Avg predicted: {df_pd['predicted_views_tomorrow'].mean():,.0f}")
                
                top_pred = df_pd.nlargest(3, 'predicted_views_tomorrow')[
                    ['title', 'view_count', 'predicted_views_tomorrow']
                ]
                print(f"\n📈 TOP 3 PREDICTED:")
                for i, (idx, row) in enumerate(top_pred.iterrows(), 1):
                    growth = ((row['predicted_views_tomorrow'] - row['view_count']) / row['view_count'] * 100)
                    print(f"   {i}. {row['title'][:40]}...")
                    print(f"      Now: {row['view_count']:,.0f} → Tomorrow: {row['predicted_views_tomorrow']:,.0f} ({growth:+.1f}%)")
            
            except Exception as e:
                print(f"⚠️  Prediction error: {e}")
                import traceback
                traceback.print_exc()
                df_pd['predicted_views_tomorrow'] = 0
            
            # ============ SAVE MONGODB ============
            if mongo_client:
                try:
                    realtime_docs = df_pd.to_dict('records')
                    for doc in realtime_docs:
                        for key, value in list(doc.items()):
                            if pd.isna(value):
                                doc[key] = None
                        doc['processing_timestamp'] = datetime.now().isoformat()
                    
                    realtime_col.insert_many(realtime_docs)
                    
                    category_stats_flat = category_stats.copy()
                    category_stats_flat.columns = ['_'.join(map(str, col)).strip('_') for col in category_stats_flat.columns]
                    category_docs = category_stats_flat.reset_index().to_dict('records')
                    for doc in category_docs:
                        doc['timestamp'] = datetime.now()
                    category_col.insert_many(category_docs)
                    
                    predictions_col.delete_many({})
                    pred_docs = df_pd.nlargest(10, 'predicted_views_tomorrow')[
                        ['video_id', 'title', 'categoryId', 'predicted_views_tomorrow', 'view_count']
                    ].to_dict('records')
                    for doc in pred_docs:
                        doc['timestamp'] = datetime.now()
                        if 'title' not in doc or doc['title'] is None:
                            doc['title'] = 'Unknown'
                        if 'categoryId' not in doc or doc['categoryId'] is None:
                            doc['categoryId'] = '24'
                    predictions_col.insert_many(pred_docs)
                    
                    print(f"\n💾 Saved {len(realtime_docs)} records")
                
                except Exception as e:
                    print(f"⚠️  MongoDB error: {e}")
            
            print(f"\n{'='*70}\n")
            batch = []

except KeyboardInterrupt:
    print("\n⚠️  Stopping...")
    consumer.close()
    spark.stop()
    if mongo_client:
        mongo_client.close()
    print("✅ Stopped")