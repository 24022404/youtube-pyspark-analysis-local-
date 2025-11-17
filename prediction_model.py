"""
Prediction Model Module
Ensemble approach: Random Forest + Prophet
Dự đoán videos có khả năng trending cao nhất ngày mai
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, avg, count, unix_timestamp,
    current_timestamp, datediff, hour, dayofweek
)
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml import Pipeline

import pandas as pd
import numpy as np

# Import config
sys.path.append(os.path.dirname(__file__))
from config import ModelConfig, MongoConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TrendingPredictor:
    """
    Ensemble model để dự đoán trending videos
    Approach 1: Classification - video sẽ trending hay không?
    Approach 2: Regression - dự đoán view count
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.classification_model = None
        self.regression_model = None
        self.feature_cols = ModelConfig.FEATURE_COLUMNS
        self.model_path = ModelConfig.MODEL_PATH
    
    def load_training_data(self, collection=MongoConfig.COLLECTION_TRENDING):
        """
        Load data từ MongoDB để train model
        """
        logger.info(f"📥 Loading training data from MongoDB: {collection}")
        
        try:
            df = self.spark.read \
                .format("mongodb") \
                .option("database", MongoConfig.DB_NAME) \
                .option("collection", collection) \
                .load()
            
            logger.info(f"✅ Loaded {df.count()} records")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            return None
    
    def prepare_training_data(self, df: DataFrame) -> DataFrame:
        """
        Chuẩn bị data cho training
        """
        logger.info("🔧 Preparing training data...")
        
        # Filter valid data
        df = df.filter(
            col("view_count").isNotNull() &
            col("likes").isNotNull() &
            col("publishedAt").isNotNull()
        )
        
        # Create target variable: will_trend_tomorrow (binary)
        # Logic: video trending nếu view_velocity cao và engagement_rate cao
        df = df.withColumn(
            "will_trend_tomorrow",
            when(
                (col("view_velocity") > 1000) & (col("engagement_rate") > 0.03),
                1
            ).otherwise(0)
        )
        
        # Create regression target: predicted_views_24h
        # Estimate views in next 24h based on current velocity
        df = df.withColumn(
            "predicted_views_24h",
            col("view_velocity") * 1440  # 1440 minutes in 24h
        )
        
        # Ensure all features exist
        required_cols = [
            'view_count', 'likes', 'comment_count', 'engagement_rate',
            'view_velocity', 'publish_hour', 'publish_day_of_week',
            'title_length', 'tags_count', 'category_name'
        ]
        
        for c in required_cols:
            if c not in df.columns:
                if c == 'view_velocity':
                    df = df.withColumn('view_velocity', lit(0))
                elif c in ['publish_hour', 'publish_day_of_week']:
                    df = df.withColumn(c, lit(0))
                elif c in ['title_length', 'tags_count']:
                    df = df.withColumn(c, lit(0))
        
        # Fill nulls
        df = df.fillna(0, subset=['view_velocity', 'engagement_rate', 'likes', 'comment_count'])
        
        logger.info(f"✅ Prepared {df.count()} training samples")
        return df
    
    def build_classification_pipeline(self) -> Pipeline:
        """
        Build classification pipeline (Random Forest)
        Target: will_trend_tomorrow (0/1)
        """
        logger.info("🏗️ Building classification pipeline...")
        
        # Encode category
        category_indexer = StringIndexer(
            inputCol="category_name",
            outputCol="category_encoded",
            handleInvalid="keep"
        )
        
        # Assemble features
        feature_cols = [
            'view_count', 'likes', 'comment_count', 'engagement_rate',
            'view_velocity', 'publish_hour', 'publish_day_of_week',
            'category_encoded', 'title_length', 'tags_count'
        ]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw",
            handleInvalid="skip"
        )
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Random Forest Classifier
        rf_classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="will_trend_tomorrow",
            predictionCol="prediction",
            probabilityCol="probability",
            numTrees=ModelConfig.RANDOM_FOREST_PARAMS['numTrees'],
            maxDepth=ModelConfig.RANDOM_FOREST_PARAMS['maxDepth'],
            minInstancesPerNode=ModelConfig.RANDOM_FOREST_PARAMS['minInstancesPerNode'],
            seed=ModelConfig.RANDOM_FOREST_PARAMS['seed']
        )
        
        pipeline = Pipeline(stages=[category_indexer, assembler, scaler, rf_classifier])
        
        logger.info("✅ Classification pipeline built")
        return pipeline
    
    def build_regression_pipeline(self) -> Pipeline:
        """
        Build regression pipeline (Random Forest)
        Target: predicted_views_24h
        """
        logger.info("🏗️ Building regression pipeline...")
        
        # Encode category
        category_indexer = StringIndexer(
            inputCol="category_name",
            outputCol="category_encoded",
            handleInvalid="keep"
        )
        
        # Assemble features
        feature_cols = [
            'view_count', 'likes', 'comment_count', 'engagement_rate',
            'view_velocity', 'publish_hour', 'publish_day_of_week',
            'category_encoded', 'title_length', 'tags_count'
        ]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw",
            handleInvalid="skip"
        )
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Random Forest Regressor
        rf_regressor = RandomForestRegressor(
            featuresCol="features",
            labelCol="predicted_views_24h",
            predictionCol="predicted_views",
            numTrees=ModelConfig.RANDOM_FOREST_PARAMS['numTrees'],
            maxDepth=ModelConfig.RANDOM_FOREST_PARAMS['maxDepth'],
            minInstancesPerNode=ModelConfig.RANDOM_FOREST_PARAMS['minInstancesPerNode'],
            seed=ModelConfig.RANDOM_FOREST_PARAMS['seed']
        )
        
        pipeline = Pipeline(stages=[category_indexer, assembler, scaler, rf_regressor])
        
        logger.info("✅ Regression pipeline built")
        return pipeline
    
    def train_classification_model(self, df: DataFrame) -> Tuple[object, Dict]:
        """
        Train classification model
        """
        logger.info("🎯 Training classification model...")
        
        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        logger.info(f"   Train: {train_df.count()}, Test: {test_df.count()}")
        
        # Build and train
        pipeline = self.build_classification_pipeline()
        model = pipeline.fit(train_df)
        
        # Evaluate
        predictions = model.transform(test_df)
        evaluator = BinaryClassificationEvaluator(
            labelCol="will_trend_tomorrow",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        
        auc = evaluator.evaluate(predictions)
        
        # Calculate accuracy
        accuracy = predictions.filter(
            col("prediction") == col("will_trend_tomorrow")
        ).count() / predictions.count()
        
        metrics = {
            'auc': float(auc),
            'accuracy': float(accuracy),
            'train_samples': train_df.count(),
            'test_samples': test_df.count(),
            'trained_at': datetime.now().isoformat()
        }
        
        logger.info(f"✅ Classification model trained - AUC: {auc:.4f}, Accuracy: {accuracy:.4f}")
        
        self.classification_model = model
        return model, metrics
    
    def train_regression_model(self, df: DataFrame) -> Tuple[object, Dict]:
        """
        Train regression model
        """
        logger.info("📈 Training regression model...")
        
        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        logger.info(f"   Train: {train_df.count()}, Test: {test_df.count()}")
        
        # Build and train
        pipeline = self.build_regression_pipeline()
        model = pipeline.fit(train_df)
        
        # Evaluate
        predictions = model.transform(test_df)
        evaluator = RegressionEvaluator(
            labelCol="predicted_views_24h",
            predictionCol="predicted_views",
            metricName="rmse"
        )
        
        rmse = evaluator.evaluate(predictions)
        
        # Calculate R2
        evaluator_r2 = RegressionEvaluator(
            labelCol="predicted_views_24h",
            predictionCol="predicted_views",
            metricName="r2"
        )
        r2 = evaluator_r2.evaluate(predictions)
        
        metrics = {
            'rmse': float(rmse),
            'r2': float(r2),
            'train_samples': train_df.count(),
            'test_samples': test_df.count(),
            'trained_at': datetime.now().isoformat()
        }
        
        logger.info(f"✅ Regression model trained - RMSE: {rmse:.2f}, R2: {r2:.4f}")
        
        self.regression_model = model
        return model, metrics
    
    def train_models(self, df: DataFrame = None) -> Dict:
        """
        Train cả 2 models
        """
        logger.info("🚀 Starting model training...")
        
        # Load data if not provided
        if df is None:
            df = self.load_training_data()
            if df is None:
                raise ValueError("Cannot load training data")
        
        # Prepare data
        df = self.prepare_training_data(df)
        
        # Train classification
        clf_model, clf_metrics = self.train_classification_model(df)
        
        # Train regression
        reg_model, reg_metrics = self.train_regression_model(df)
        
        # Save models
        self.save_models(clf_metrics, reg_metrics)
        
        results = {
            'classification': clf_metrics,
            'regression': reg_metrics,
            'status': 'success'
        }
        
        logger.info("✅ All models trained successfully")
        return results
    
    def predict_trending_tomorrow(self, current_videos_df: DataFrame) -> DataFrame:
        """
        Dự đoán videos sẽ trending ngày mai
        """
        logger.info("🔮 Predicting tomorrow's trending videos...")
        
        if self.classification_model is None or self.regression_model is None:
            logger.error("❌ Models not trained yet!")
            return None
        
        # Classification predictions
        clf_predictions = self.classification_model.transform(current_videos_df)
        
        # Regression predictions
        reg_predictions = self.regression_model.transform(clf_predictions)
        
        # Combine predictions
        predictions = reg_predictions.withColumn(
            "trending_score",
            col("probability").getItem(1)  # Probability of class 1
        )
        
        # Select top predictions
        top_predictions = predictions.select(
            "video_id",
            "title",
            "category_name",
            "view_count",
            "view_velocity",
            "engagement_rate",
            "trending_score",
            "predicted_views",
            "prediction"
        ).filter(
            col("trending_score") > ModelConfig.PREDICTION_THRESHOLD
        ).orderBy(
            col("trending_score").desc()
        ).limit(50)
        
        logger.info(f"✅ Predicted {top_predictions.count()} potential trending videos")
        return top_predictions
    
    def save_models(self, clf_metrics: Dict, reg_metrics: Dict):
        """
        Save models và metrics
        """
        logger.info(f"💾 Saving models to {self.model_path}...")
        
        try:
            # Create directory
            os.makedirs(self.model_path, exist_ok=True)
            
            # Save classification model
            clf_path = f"{self.model_path}/classification"
            self.classification_model.write().overwrite().save(clf_path)
            logger.info(f"   ✅ Classification model saved to {clf_path}")
            
            # Save regression model
            reg_path = f"{self.model_path}/regression"
            self.regression_model.write().overwrite().save(reg_path)
            logger.info(f"   ✅ Regression model saved to {reg_path}")
            
            # Save metrics
            metrics_path = f"{self.model_path}/metrics.json"
            with open(metrics_path, 'w') as f:
                json.dump({
                    'classification': clf_metrics,
                    'regression': reg_metrics
                }, f, indent=2)
            logger.info(f"   ✅ Metrics saved to {metrics_path}")
            
        except Exception as e:
            logger.error(f"❌ Error saving models: {e}")
    
    def load_models(self):
        """
        Load trained models
        """
        logger.info(f"📥 Loading models from {self.model_path}...")
        
        try:
            from pyspark.ml import PipelineModel
            
            # Load classification
            clf_path = f"{self.model_path}/classification"
            self.classification_model = PipelineModel.load(clf_path)
            logger.info("   ✅ Classification model loaded")
            
            # Load regression
            reg_path = f"{self.model_path}/regression"
            self.regression_model = PipelineModel.load(reg_path)
            logger.info("   ✅ Regression model loaded")
            
            # Load metrics
            metrics_path = f"{self.model_path}/metrics.json"
            with open(metrics_path, 'r') as f:
                metrics = json.load(f)
            
            logger.info("✅ Models loaded successfully")
            return metrics
            
        except Exception as e:
            logger.error(f"❌ Error loading models: {e}")
            return None


# ========================================
# PROPHET TIME SERIES (Optional)
# ========================================
class ProphetPredictor:
    """
    Use Facebook Prophet for time series forecasting
    Dự đoán view count growth patterns
    """
    
    def __init__(self):
        try:
            from prophet import Prophet
            self.prophet = Prophet
            logger.info("✅ Prophet available")
        except ImportError:
            logger.warning("⚠️  Prophet not available, install with: pip install prophet")
            self.prophet = None
    
    def prepare_prophet_data(self, df: DataFrame) -> pd.DataFrame:
        """
        Prepare data for Prophet (requires pandas)
        """
        # Convert to pandas
        pdf = df.select("trending_date", "view_count").toPandas()
        
        # Group by date and sum views
        pdf = pdf.groupby('trending_date')['view_count'].sum().reset_index()
        pdf.columns = ['ds', 'y']
        
        return pdf
    
    def train_prophet(self, pdf: pd.DataFrame) -> object:
        """
        Train Prophet model
        """
        if self.prophet is None:
            return None
        
        logger.info("📈 Training Prophet model...")
        
        model = self.prophet(**ModelConfig.PROPHET_PARAMS)
        model.fit(pdf)
        
        logger.info("✅ Prophet model trained")
        return model
    
    def predict_future(self, model, periods=1) -> pd.DataFrame:
        """
        Predict next N days
        """
        future = model.make_future_dataframe(periods=periods)
        forecast = model.predict(future)
        
        return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(periods)


# ========================================
# MAIN
# ========================================
def main():
    """
    Main entry point for training
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Train YouTube Trending Prediction Models')
    parser.add_argument('--mode', choices=['train', 'predict'], default='train',
                        help='Mode: train or predict')
    parser.add_argument('--load', action='store_true',
                        help='Load existing models instead of training')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("YOUTUBE TRENDING PREDICTION")
    logger.info("="*60)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TrendingPrediction") \
        .master("local[*]") \
        .config("spark.mongodb.read.connection.uri", MongoConfig.URI) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Create predictor
    predictor = TrendingPredictor(spark)
    
    if args.mode == 'train':
        if args.load:
            logger.info("📥 Loading existing models...")
            metrics = predictor.load_models()
            if metrics:
                logger.info(f"Loaded models metrics: {json.dumps(metrics, indent=2)}")
        else:
            logger.info("🎯 Training new models...")
            results = predictor.train_models()
            logger.info(f"Training results: {json.dumps(results, indent=2)}")
    
    elif args.mode == 'predict':
        logger.info("🔮 Making predictions...")
        predictor.load_models()
        
        # Load current trending videos
        current_df = predictor.load_training_data()
        if current_df:
            predictions = predictor.predict_trending_tomorrow(current_df)
            if predictions:
                predictions.show(20, truncate=False)
    
    spark.stop()
    logger.info("✅ Done")


if __name__ == '__main__':
    main()
