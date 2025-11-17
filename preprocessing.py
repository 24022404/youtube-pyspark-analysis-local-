"""
Preprocessing Module
Extract và tái sử dụng logic từ file 01_preprocessing_final.ipynb
Xử lý data: clean, transform, feature engineering
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, when, explode, length, size, split,
    hour, dayofweek, datediff, lit, regexp_replace, coalesce,
    unix_timestamp, from_unixtime
)
from functools import reduce
from config import YouTubeConfig
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YouTubeDataPreprocessor:
    """
    Class xử lý preprocessing cho YouTube data
    Tái sử dụng logic từ notebook 01
    """
    
    def __init__(self, spark):
        self.spark = spark
        self.category_map = YouTubeConfig.CATEGORY_MAP
    
    def load_category_mapping(self, json_path):
        """
        Load category mapping từ JSON file
        """
        try:
            df_json = self.spark.read.json(json_path, multiLine=True)
            df_categories = df_json.select(explode(col("items")).alias("item")) \
                .select(
                    col("item.id").alias("categoryId"),
                    col("item.snippet.title").alias("category_name")
                )
            logger.info(f"✅ Loaded {df_categories.count()} categories")
            return df_categories
        except Exception as e:
            logger.error(f"❌ Error loading categories: {e}")
            return None
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Bước 1: Làm sạch dữ liệu
        - Xóa rows có tất cả giá trị null
        - Xóa rows có video_id null
        - Fill null cho description
        """
        logger.info("🧹 Cleaning data...")
        
        # Xóa rows có tất cả giá trị null
        df = df.filter(
            reduce(lambda a, b: a | b, (col(c).isNotNull() for c in df.columns))
        )
        
        # Xóa rows không có video_id
        df = df.filter(col("video_id").isNotNull() & (col("video_id") != ""))
        
        # Fill null cho description
        df = df.fillna({"description": "No description"})
        
        # Drop duplicates
        df = df.dropDuplicates(["video_id", "trending_date"])
        
        logger.info(f"✅ Cleaned data: {df.count()} rows")
        return df
    
    def fix_timestamps(self, df: DataFrame) -> DataFrame:
        """
        Bước 2: Sửa timestamp format
        Từ file 01: trending_date có format 'yyyy-MM-dd HH:mm:ss' thay vì ISO format
        """
        logger.info("🕐 Fixing timestamps...")
        
        # Fix trending_date: từ 'yyyy-MM-dd HH:mm:ss' -> timestamp
        df = df.withColumn(
            'trending_date',
            to_timestamp(col('trending_date'), 'yyyy-MM-dd HH:mm:ss')
        )
        
        # Fix publishedAt: từ 'yyyy-MM-dd HH:mm:ss' -> timestamp
        df = df.withColumn(
            'publishedAt',
            to_timestamp(col('publishedAt'), 'yyyy-MM-dd HH:mm:ss')
        )
        
        # Lọc bỏ các rows có timestamp không hợp lệ
        df = df.filter(
            col('trending_date').isNotNull() & col('publishedAt').isNotNull()
        )
        
        logger.info("✅ Timestamps fixed")
        return df
    
    def add_category_names(self, df: DataFrame, category_df: DataFrame = None) -> DataFrame:
        """
        Bước 3: Thêm category names
        """
        logger.info("🏷️ Adding category names...")
        
        if category_df is None:
            # Tạo category mapping từ config
            from pyspark.sql.types import StructType, StructField, StringType
            schema = StructType([
                StructField("categoryId", StringType(), True),
                StructField("category_name", StringType(), True)
            ])
            category_data = [(k, v) for k, v in self.category_map.items()]
            category_df = self.spark.createDataFrame(category_data, schema)
        
        # Convert categoryId to string for joining
        df = df.withColumn("categoryId", col("categoryId").cast("string"))
        
        # Join với categories
        df = df.join(
            category_df,
            df.categoryId == category_df.categoryId,
            "left"
        ).drop(category_df.categoryId)
        
        logger.info("✅ Category names added")
        return df
    
    def feature_engineering(self, df: DataFrame) -> DataFrame:
        """
        Bước 4: Feature Engineering
        Tạo các features mới cho ML model
        """
        logger.info("⚙️ Feature engineering...")
        
        # 1. Engagement rate
        df = df.withColumn(
            "engagement_rate",
            when(col("view_count") > 0,
                 (col("likes") + col("comment_count")) / col("view_count")
            ).otherwise(0)
        )
        
        # 2. Like ratio
        df = df.withColumn(
            "like_ratio",
            when(col("view_count") > 0,
                 col("likes") / col("view_count")
            ).otherwise(0)
        )
        
        # 3. Comment ratio
        df = df.withColumn(
            "comment_ratio",
            when(col("view_count") > 0,
                 col("comment_count") / col("view_count")
            ).otherwise(0)
        )
        
        # 4. Time features từ publishedAt
        df = df.withColumn("publish_hour", hour(col("publishedAt")))
        df = df.withColumn("publish_day_of_week", dayofweek(col("publishedAt")))
        
        # 5. Days to trending (từ publish đến trending)
        df = df.withColumn(
            "days_to_trending",
            datediff(col("trending_date"), col("publishedAt"))
        )
        
        # 6. Title length
        df = df.withColumn("title_length", length(col("title")))
        
        # 7. Tags count
        df = df.withColumn(
            "tags_count",
            when(col("tags").isNotNull(),
                 size(split(col("tags"), "\\|"))
            ).otherwise(0)
        )
        
        # 8. Description length
        df = df.withColumn("description_length", length(col("description")))
        
        # 9. Has tags flag
        df = df.withColumn(
            "has_tags",
            when(col("tags").isNotNull() & (col("tags") != ""), 1).otherwise(0)
        )
        
        logger.info("✅ Feature engineering completed")
        return df
    
    def preprocess_pipeline(self, df: DataFrame, category_json_path: str = None) -> DataFrame:
        """
        Pipeline đầy đủ: Clean -> Fix timestamps -> Add categories -> Feature engineering
        """
        logger.info("🚀 Starting preprocessing pipeline...")
        
        # Load categories nếu có
        category_df = None
        if category_json_path:
            category_df = self.load_category_mapping(category_json_path)
        
        # Apply pipeline
        df = self.clean_data(df)
        df = self.fix_timestamps(df)
        df = self.add_category_names(df, category_df)
        df = self.feature_engineering(df)
        
        logger.info(f"✅ Preprocessing completed: {df.count()} rows, {len(df.columns)} columns")
        return df
    
    def preprocess_realtime_record(self, record: dict) -> dict:
        """
        Xử lý một record real-time từ Kafka
        Lightweight preprocessing cho streaming data
        """
        try:
            # Parse timestamps
            from datetime import datetime
            
            if 'publishedAt' in record:
                record['publishedAt'] = datetime.fromisoformat(
                    record['publishedAt'].replace('Z', '+00:00')
                )
            
            # Add current timestamp as trending_date
            record['trending_date'] = datetime.now()
            
            # Calculate engagement rate
            view_count = record.get('view_count', 0)
            likes = record.get('likes', 0)
            comments = record.get('comment_count', 0)
            
            if view_count > 0:
                record['engagement_rate'] = (likes + comments) / view_count
                record['like_ratio'] = likes / view_count
                record['comment_ratio'] = comments / view_count
            else:
                record['engagement_rate'] = 0
                record['like_ratio'] = 0
                record['comment_ratio'] = 0
            
            # Add category name
            category_id = str(record.get('categoryId', ''))
            record['category_name'] = self.category_map.get(category_id, 'Unknown')
            
            # Title length
            record['title_length'] = len(record.get('title', ''))
            
            # Tags count
            tags = record.get('tags', '')
            record['tags_count'] = len(tags.split('|')) if tags else 0
            
            # Description length
            record['description_length'] = len(record.get('description', ''))
            
            return record
            
        except Exception as e:
            logger.error(f"Error preprocessing record: {e}")
            return record


def get_ml_features(df: DataFrame) -> DataFrame:
    """
    Chọn features cần thiết cho ML model
    """
    feature_cols = [
        'video_id',
        'title',
        'channelTitle',
        'category_name',
        'view_count',
        'likes',
        'comment_count',
        'engagement_rate',
        'like_ratio',
        'comment_ratio',
        'publish_hour',
        'publish_day_of_week',
        'days_to_trending',
        'title_length',
        'tags_count',
        'description_length',
        'trending_date',
        'publishedAt'
    ]
    
    return df.select(*[col for col in feature_cols if col in df.columns])


# ========================================
# HELPER FUNCTIONS
# ========================================
def dataframe_info(df: DataFrame, name: str = "DataFrame"):
    """
    Hiển thị thông tin DataFrame (tương tự notebook 01)
    """
    print("=" * 60)
    print(f"{name} INFO")
    print("=" * 60)
    print(f"Rows: {df.count()}, Columns: {len(df.columns)}")
    print("\nSchema:")
    df.printSchema()
    print("\nNull counts:")
    df.select([
        (col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]).agg(*[
        sum(col(c)).alias(c) for c in df.columns
    ]).show()
    print("=" * 60)


if __name__ == '__main__':
    # Test preprocessing
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("PreprocessingTest") \
        .master("local[*]") \
        .getOrCreate()
    
    preprocessor = YouTubeDataPreprocessor(spark)
    print("✅ Preprocessor initialized successfully")
    
    spark.stop()