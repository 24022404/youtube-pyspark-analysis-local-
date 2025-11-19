"""
07_realtime_analysis.py
Phân tích real-time từ Kafka (KHÔNG dùng Spark Streaming - tránh lỗi Windows)
Dùng Kafka Consumer + PySpark batch processing (giống file 02-05)
"""

import os
import sys
from datetime import datetime
from kafka import KafkaConsumer
import json
import pandas as pd
from pymongo import MongoClient
import warnings
warnings.filterwarnings('ignore')

# Setup Java (GIỐNG FILE 02-05)
os.environ['JAVA_HOME'] = 'C:\\Java\\jdk-1.8'
os.environ['HADOOP_HOME'] = os.environ.get('JAVA_HOME')
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}\\bin;{os.environ.get('PATH', '')}"

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
import tempfile

print("=" * 70)
print("YOUTUBE REAL-TIME ANALYSIS (Windows-friendly)")
print("=" * 70)

# =====================================================================
# 1. KHỞI TẠO SPARK SESSION (GIỐNG FILE 02-05)
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
# 2. TRAIN MÔ HÌNH
# =====================================================================
print("\n[2/4] Training ML Model...")

train_df = spark.read.csv("./data/preprocessed_data.csv", header=True, inferSchema=True)
print(f"   Loaded {train_df.count():,} training samples")

# Prepare features
feature_cols = ['categoryId', 'view_count', 'likes', 'comment_count']
train_clean = train_df.select(feature_cols).na.drop()

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="view_count",
    numTrees=50,
    maxDepth=10,
    seed=42
)

pipeline = Pipeline(stages=[assembler, rf])
model = pipeline.fit(train_clean)
print("✅ Model trained")

# =====================================================================
# 3. KẾT NỐI MONGODB
# =====================================================================
print("\n[3/4] Connecting to MongoDB...")

try:
    mongo_client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
    mongo_client.server_info()  # Test connection
    
    db = mongo_client['youtube_analytics']
    realtime_col = db['realtime_data']
    category_col = db['category_stats']
    predictions_col = db['predictions']
    
    print("✅ MongoDB connected")
except Exception as e:
    print(f"⚠️  MongoDB not available: {e}")
    print("   Continuing without MongoDB...")
    mongo_client = None

# =====================================================================
# 4. KAFKA CONSUMER (GIỐNG PRODUCER)
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

print("✅ Kafka consumer connected")
print("\n" + "=" * 70)
print("🚀 REAL-TIME PROCESSING STARTED")
print("=" * 70)
print("\nListening to Kafka topic: youtube-trending")
print("Processing batch every 10 messages...\n")

# =====================================================================
# 5. XỬ LÝ REAL-TIME
# =====================================================================
batch = []
batch_count = 0
BATCH_SIZE = 10

try:
    for message in consumer:
        data = message.value
        batch.append(data)
        
        # Xử lý mỗi batch
        if len(batch) >= BATCH_SIZE:
            batch_count += 1
            
            print(f"\n{'='*70}")
            print(f"BATCH {batch_count} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*70}")
            
            # Convert to DataFrame
            df_pd = pd.DataFrame(batch)
            
            # ============ PHÂN TÍCH ============
            print(f"\n📊 REAL-TIME ANALYSIS:")
            print(f"   Videos: {len(df_pd)}")
            print(f"   Total views: {df_pd['view_count'].sum():,}")
            
            # Engagement rate
            df_pd['engagement_rate'] = (
                (df_pd['likes'] + df_pd['comment_count']) / df_pd['view_count'] * 100
            )
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
            for idx, row in top_videos.iterrows():
                print(f"   {idx+1}. {row['title'][:50]}... | {row['view_count']:,} views")
            
            # ============ DỰ ĐOÁN ============
            try:
                # BYPASS Spark createDataFrame - predict manually using sklearn style
                from pyspark.ml.linalg import Vectors
                
                predictions_list = []
                for idx, row in df_pd.iterrows():
                    # Create feature vector manually
                    features = [
                        float(row['categoryId']),
                        float(row['view_count']),
                        float(row['likes']),
                        float(row['comment_count'])
                    ]
                    
                    # Create single-row Spark DF
                    single_row = spark.createDataFrame(
                        [(Vectors.dense(features),)],
                        ["features"]
                    )
                    
                    # Predict
                    pred = model.stages[-1].transform(single_row).select("prediction").first()[0]
                    predictions_list.append(pred)
                
                df_pd['predicted_views_tomorrow'] = predictions_list

                if 'categoryId' not in df_pd.columns:
                    df_pd['categoryId'] = '24' 
                
                print(f"\n🔮 PREDICTIONS:")
                print(f"   Avg predicted views tomorrow: {df_pd['predicted_views_tomorrow'].mean():,.0f}")
                
                top_pred = df_pd.nlargest(3, 'predicted_views_tomorrow')[
                    ['title', 'view_count', 'predicted_views_tomorrow']
                ]
                print(f"\n📈 TOP 3 PREDICTED GROWTH:")
                for i, (idx, row) in enumerate(top_pred.iterrows(), 1):  # FIX: enumerate properly
                    growth = ((row['predicted_views_tomorrow'] - row['view_count']) / row['view_count'] * 100)
                    print(f"   {i}. {row['title'][:40]}...")
                    print(f"      Now: {row['view_count']:,.0f} → Tomorrow: {row['predicted_views_tomorrow']:,.0f} (+{growth:.1f}%)")
            
            except Exception as e:
                print(f"⚠️  Prediction error: {e}")
                df_pd['predicted_views_tomorrow'] = 0
            
            # ============ LƯU MONGODB ============
            if mongo_client:
                try:
                    # Realtime data
                    realtime_docs = df_pd.to_dict('records')
                    for doc in realtime_docs:
                        for key, value in list(doc.items()):  # FIX: items() thay vì iteritems()
                            if pd.isna(value):
                                doc[key] = None
                        doc['processing_timestamp'] = datetime.now() 
                    
                    realtime_col.insert_many(realtime_docs)
                    
                    # Category stats - FIX: flatten MultiIndex columns
                    category_stats_flat = category_stats.copy()
                    category_stats_flat.columns = ['_'.join(map(str, col)).strip('_') for col in category_stats_flat.columns]
                    category_docs = category_stats_flat.reset_index().to_dict('records')
                    for doc in category_docs:
                        doc['timestamp'] = datetime.now()
                    category_col.insert_many(category_docs)
                    
                    # Predictions
                    predictions_col.delete_many({}) 
                    pred_docs = df_pd.nlargest(10, 'predicted_views_tomorrow')[
                        ['video_id', 'title', 'categoryId', 'predicted_views_tomorrow', 'view_count']
                    ].to_dict('records')
                    for doc in pred_docs:
                        doc['timestamp'] = datetime.now()
                        if 'title' not in doc or doc['title'] is None:
                            doc['title'] = 'Unknown Title'
                        if 'categoryId' not in doc or doc['categoryId'] is None:
                            doc['categoryId'] = '24'
                    predictions_col.insert_many(pred_docs)
                    
                    
                    print(f"\n💾 Saved to MongoDB: {len(realtime_docs)} records")
                
                except Exception as e:
                    print(f"⚠️  MongoDB save error: {e}")
            
            print(f"\n{'='*70}\n")
            
            # Clear batch
            batch = []

except KeyboardInterrupt:
    print("\n\n⚠️  Stopping...")
    consumer.close()
    spark.stop()
    if mongo_client:
        mongo_client.close()
    print("✅ Stopped gracefully")
