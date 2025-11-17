"""
Spark Streaming Application
Đọc data từ Kafka → Preprocessing → Analysis → Prediction → Write to MongoDB
"""
import os
import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    window, count, avg, sum as spark_sum, max as spark_max,
    struct, to_json, explode, lit, unix_timestamp, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)

# Import modules
sys.path.append(os.path.dirname(__file__))
from config import KafkaConfig, MongoConfig, SparkConfig, AnalysisConfig
from preprocessing import YouTubeDataPreprocessor
from analysis import YouTubeAnalyzer

# Setup logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class YouTubeStreamProcessor:
    """
    Main Spark Streaming application
    """
    
    def __init__(self):
        self.spark = None
        self.preprocessor = None
        self.analyzer = None
        
        self._init_spark_session()
        self._init_processors()
    
    def _init_spark_session(self):
        """
        Initialize Spark Session with Kafka and MongoDB support
        """
        logger.info("🚀 Initializing Spark Session...")
        
        try:
            self.spark = SparkSession.builder \
                .appName(SparkConfig.APP_NAME) \
                .master(SparkConfig.MASTER_URL) \
                .config("spark.driver.memory", SparkConfig.DRIVER_MEMORY) \
                .config("spark.executor.memory", SparkConfig.EXECUTOR_MEMORY) \
                .config("spark.mongodb.write.connection.uri", MongoConfig.URI) \
                .config("spark.mongodb.read.connection.uri", MongoConfig.URI) \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
                .config("spark.streaming.stopGracefullyOnShutdown", "true") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"✅ Spark {self.spark.version} session created")
            logger.info(f"   Master: {SparkConfig.MASTER_URL}")
            logger.info(f"   MongoDB: {MongoConfig.URI}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark: {e}")
            raise
    
    def _init_processors(self):
        """
        Initialize preprocessor and analyzer
        """
        self.preprocessor = YouTubeDataPreprocessor(self.spark)
        self.analyzer = YouTubeAnalyzer()
        logger.info("✅ Processors initialized")
    
    def get_kafka_schema(self):
        """
        Define schema cho Kafka messages
        """
        return StructType([
            StructField("video_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("publishedAt", StringType(), True),
            StructField("channelId", StringType(), True),
            StructField("channelTitle", StringType(), True),
            StructField("categoryId", StringType(), True),
            StructField("category_name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("thumbnail_link", StringType(), True),
            StructField("view_count", IntegerType(), True),
            StructField("likes", IntegerType(), True),
            StructField("dislikes", IntegerType(), True),
            StructField("comment_count", IntegerType(), True),
            StructField("comments_disabled", BooleanType(), True),
            StructField("ratings_disabled", BooleanType(), True),
            StructField("engagement_rate", DoubleType(), True),
            StructField("fetch_timestamp", StringType(), True),
            StructField("trending_date", StringType(), True)
        ])
    
    def read_from_kafka(self):
        """
        Read streaming data from Kafka
        """
        logger.info(f"📡 Connecting to Kafka: {KafkaConfig.BOOTSTRAP_SERVERS}")
        logger.info(f"   Topic: {KafkaConfig.TOPIC_TRENDING}")
        
        try:
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KafkaConfig.BOOTSTRAP_SERVERS) \
                .option("subscribe", KafkaConfig.TOPIC_TRENDING) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            logger.info("✅ Kafka stream connected")
            return kafka_df
            
        except Exception as e:
            logger.error(f"❌ Failed to read from Kafka: {e}")
            raise
    
    def parse_kafka_messages(self, kafka_df):
        """
        Parse Kafka messages from JSON
        """
        schema = self.get_kafka_schema()
        
        # Parse value from JSON
        parsed_df = kafka_df.select(
            col("key").cast("string").alias("video_id_key"),
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # Flatten struct
        parsed_df = parsed_df.select(
            "video_id_key",
            "kafka_timestamp",
            "data.*"
        )
        
        # Convert timestamps
        parsed_df = parsed_df \
            .withColumn("publishedAt", to_timestamp(col("publishedAt"))) \
            .withColumn("trending_date", to_timestamp(col("trending_date"))) \
            .withColumn("fetch_timestamp", to_timestamp(col("fetch_timestamp")))
        
        logger.info("✅ Kafka messages parsed")
        return parsed_df
    
    def add_streaming_features(self, df):
        """
        Add features specific to streaming data
        """
        # Time features
        df = df.withColumn("publish_hour", 
                          col("publishedAt").cast("string").substr(12, 2).cast("int"))
        df = df.withColumn("publish_day_of_week", 
                          ((unix_timestamp(col("publishedAt")) / 86400 + 4) % 7).cast("int"))
        
        # Days to trending (from publish to now)
        df = df.withColumn(
            "days_to_trending",
            ((unix_timestamp(current_timestamp()) - unix_timestamp(col("publishedAt"))) / 86400).cast("int")
        )
        
        # View velocity (views per minute)
        df = df.withColumn(
            "minutes_since_publish",
            (unix_timestamp(current_timestamp()) - unix_timestamp(col("publishedAt"))) / 60
        )
        
        df = df.withColumn(
            "view_velocity",
            when(col("minutes_since_publish") > 0,
                 col("view_count") / col("minutes_since_publish")
            ).otherwise(0)
        )
        
        # Like/comment ratios
        df = df.withColumn(
            "like_ratio",
            when(col("view_count") > 0, col("likes") / col("view_count")).otherwise(0)
        )
        
        df = df.withColumn(
            "comment_ratio",
            when(col("view_count") > 0, col("comment_count") / col("view_count")).otherwise(0)
        )
        
        # Title length
        df = df.withColumn("title_length", col("title").cast("string").length())
        
        # Tags count
        df = df.withColumn(
            "tags_count",
            when(col("tags").isNotNull(), 
                 col("tags").cast("string").split("\\|").size()
            ).otherwise(0)
        )
        
        return df
    
    def write_to_mongodb(self, df, collection_name, mode="append"):
        """
        Write DataFrame to MongoDB
        """
        def write_batch(batch_df, batch_id):
            try:
                logger.info(f"💾 Writing batch {batch_id} to MongoDB ({batch_df.count()} records)")
                
                batch_df.write \
                    .format("mongodb") \
                    .mode(mode) \
                    .option("database", MongoConfig.DB_NAME) \
                    .option("collection", collection_name) \
                    .save()
                
                logger.info(f"✅ Batch {batch_id} written successfully")
                
            except Exception as e:
                logger.error(f"❌ Error writing batch {batch_id}: {e}")
        
        return write_batch
    
    def aggregate_windowed_analytics(self, df):
        """
        Tính toán analytics theo tumbling window (1 minute)
        """
        # Tumbling window: 1 minute
        windowed_df = df \
            .withWatermark("kafka_timestamp", "2 minutes") \
            .groupBy(
                window(col("kafka_timestamp"), "1 minute"),
                col("category_name")
            ) \
            .agg(
                count("*").alias("video_count"),
                avg("view_count").alias("avg_views"),
                avg("likes").alias("avg_likes"),
                avg("engagement_rate").alias("avg_engagement"),
                spark_max("view_count").alias("max_views"),
                avg("view_velocity").alias("avg_view_velocity")
            )
        
        # Flatten window struct
        windowed_df = windowed_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("category_name"),
            col("video_count"),
            col("avg_views"),
            col("avg_likes"),
            col("avg_engagement"),
            col("max_views"),
            col("avg_view_velocity")
        )
        
        return windowed_df
    
    def create_analytics_snapshot(self, df):
        """
        Tạo snapshot analytics tổng hợp
        """
        # Overall stats
        overall_stats = df.agg(
            count("*").alias("total_videos"),
            avg("view_count").alias("avg_views"),
            avg("engagement_rate").alias("avg_engagement"),
            spark_max("view_count").alias("max_views")
        )
        
        # Top categories
        top_categories = df.groupBy("category_name") \
            .agg(count("*").alias("count")) \
            .orderBy(col("count").desc()) \
            .limit(5)
        
        # Combine into single row for snapshot
        snapshot = overall_stats.crossJoin(
            top_categories.select(
                struct("category_name", "count").alias("top_category")
            ).groupBy().agg(
                collect_list("top_category").alias("top_categories")
            )
        )
        
        snapshot = snapshot.withColumn("snapshot_timestamp", current_timestamp())
        
        return snapshot
    
    def process_stream(self):
        """
        Main streaming processing logic
        """
        logger.info("🚀 Starting stream processing...")
        
        try:
            # 1. Read from Kafka
            kafka_df = self.read_from_kafka()
            
            # 2. Parse messages
            parsed_df = self.parse_kafka_messages(kafka_df)
            
            # 3. Add features
            featured_df = self.add_streaming_features(parsed_df)
            
            # 4. Write raw trending videos to MongoDB
            logger.info(f"📝 Writing to MongoDB collection: {MongoConfig.COLLECTION_TRENDING}")
            
            query_trending = featured_df.writeStream \
                .foreachBatch(
                    self.write_to_mongodb(featured_df, MongoConfig.COLLECTION_TRENDING)
                ) \
                .outputMode("append") \
                .option("checkpointLocation", "/tmp/spark-checkpoints/trending") \
                .start()
            
            # 5. Aggregate windowed analytics
            windowed_analytics = self.aggregate_windowed_analytics(featured_df)
            
            logger.info(f"📝 Writing windowed analytics to MongoDB: {MongoConfig.COLLECTION_ANALYTICS}")
            
            query_analytics = windowed_analytics.writeStream \
                .foreachBatch(
                    self.write_to_mongodb(windowed_analytics, MongoConfig.COLLECTION_ANALYTICS)
                ) \
                .outputMode("append") \
                .option("checkpointLocation", "/tmp/spark-checkpoints/analytics") \
                .start()
            
            # 6. Console output for monitoring (optional)
            query_console = featured_df.writeStream \
                .format("console") \
                .outputMode("append") \
                .option("truncate", "false") \
                .option("numRows", 3) \
                .start()
            
            logger.info("✅ All streams started successfully")
            logger.info("📊 Monitoring streams...")
            logger.info("   Press Ctrl+C to stop")
            
            # Wait for termination
            query_trending.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("\n🛑 Received stop signal")
            self.stop()
        except Exception as e:
            logger.error(f"❌ Stream processing error: {e}")
            self.stop()
            raise
    
    def stop(self):
        """
        Stop Spark session gracefully
        """
        logger.info("🛑 Stopping Spark session...")
        if self.spark:
            self.spark.stop()
            logger.info("✅ Spark session stopped")


# ========================================
# HELPER FUNCTIONS
# ========================================
def load_baselines():
    """
    Load baseline data from JSON files (from file 02-04)
    """
    baselines = {}
    baseline_path = "/opt/spark-data/baselines"
    
    for key in ['category', 'time', 'interaction']:
        path = f"{baseline_path}/{key}_baseline.json"
        try:
            with open(path, 'r') as f:
                baselines[key] = json.load(f)
            logger.info(f"✅ Loaded {key} baseline")
        except FileNotFoundError:
            logger.warning(f"⚠️  {key} baseline not found")
            baselines[key] = None
    
    return baselines


def test_mongodb_connection():
    """
    Test MongoDB connection
    """
    try:
        from pymongo import MongoClient
        
        logger.info("🧪 Testing MongoDB connection...")
        client = MongoClient(MongoConfig.URI)
        
        # Ping
        client.admin.command('ping')
        
        # List databases
        dbs = client.list_database_names()
        logger.info(f"✅ MongoDB connected. Databases: {dbs}")
        
        client.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        return False


# ========================================
# MAIN
# ========================================
def main():
    """
    Main entry point
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='YouTube Spark Streaming')
    parser.add_argument('--test-mongo', action='store_true',
                        help='Test MongoDB connection only')
    
    args = parser.parse_args()
    
    # Print config
    logger.info("="*60)
    logger.info("YOUTUBE SPARK STREAMING")
    logger.info("="*60)
    logger.info(f"Spark Master: {SparkConfig.MASTER_URL}")
    logger.info(f"Kafka Servers: {KafkaConfig.BOOTSTRAP_SERVERS}")
    logger.info(f"MongoDB URI: {MongoConfig.URI}")
    logger.info(f"Input Topic: {KafkaConfig.TOPIC_TRENDING}")
    logger.info(f"Collections: {MongoConfig.COLLECTION_TRENDING}, {MongoConfig.COLLECTION_ANALYTICS}")
    logger.info("="*60)
    
    # Test mode
    if args.test_mongo:
        test_mongodb_connection()
        return
    
    # Create and run processor
    try:
        processor = YouTubeStreamProcessor()
        processor.process_stream()
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
