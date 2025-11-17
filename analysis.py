"""
Analysis Module
Extract và tái sử dụng logic từ file 02-04 (category, time, interaction analysis)
Các functions này sẽ được gọi bởi Spark Streaming để phân tích real-time
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, max as spark_max, min as spark_min,
    hour, dayofweek, when, lit, desc, asc, approx_count_distinct
)
import pandas as pd
from typing import Dict, List, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ========================================
# CATEGORY ANALYSIS (from file 02)
# ========================================
class CategoryAnalyzer:
    """
    Phân tích category distribution và trends
    """
    
    @staticmethod
    def analyze_category_distribution(df: DataFrame) -> Dict:
        """
        Phân tích phân phối theo category
        """
        logger.info("📊 Analyzing category distribution...")
        
        # Count by category
        category_counts = df.groupBy('category_name') \
            .agg(
                count('*').alias('video_count'),
                spark_sum('view_count').alias('total_views'),
                avg('view_count').alias('avg_views'),
                avg('likes').alias('avg_likes'),
                avg('engagement_rate').alias('avg_engagement')
            ) \
            .orderBy(desc('video_count'))
        
        # Convert to dict for easy access
        result = {
            'distribution': category_counts.toPandas().to_dict('records'),
            'total_categories': category_counts.count(),
            'top_category': category_counts.first()['category_name'] if category_counts.count() > 0 else None
        }
        
        logger.info(f"✅ Found {result['total_categories']} categories")
        return result
    
    @staticmethod
    def compare_with_baseline(current_df: DataFrame, baseline: Dict) -> Tuple[pd.DataFrame, List[Dict]]:
        """
        So sánh distribution hiện tại với baseline
        Detect anomalies
        """
        if baseline is None:
            return None, []
        
        # Current distribution
        current_dist = current_df.groupBy('category_name').count().toPandas()
        current_dist['percentage'] = current_dist['count'] / current_dist['count'].sum() * 100
        
        # Baseline distribution
        baseline_pct = baseline.get('category_distribution', {}).get('by_percentage', {})
        
        # Compare
        anomalies = []
        comparison_data = []
        
        for _, row in current_dist.iterrows():
            category = row['category_name']
            current_pct = row['percentage']
            baseline_pct_val = baseline_pct.get(category, 0)
            
            change = current_pct - baseline_pct_val
            change_ratio = (change / baseline_pct_val * 100) if baseline_pct_val > 0 else 0
            
            comparison_data.append({
                'category': category,
                'baseline_%': baseline_pct_val,
                'current_%': current_pct,
                'change_%': change,
                'change_ratio': change_ratio
            })
            
            # Detect anomaly (>20% change)
            if abs(change_ratio) > 20:
                anomalies.append({
                    'type': 'CATEGORY',
                    'category': category,
                    'direction': '📈 SURGE' if change_ratio > 0 else '📉 DECLINE',
                    'change_ratio': change_ratio,
                    'current': current_pct,
                    'baseline': baseline_pct_val
                })
        
        comparison_df = pd.DataFrame(comparison_data).sort_values('change_ratio', ascending=False)
        return comparison_df, anomalies
    
    @staticmethod
    def get_top_categories(df: DataFrame, n: int = 5) -> List[Dict]:
        """
        Lấy top N categories
        """
        top_cats = df.groupBy('category_name') \
            .agg(
                count('*').alias('count'),
                avg('view_count').alias('avg_views')
            ) \
            .orderBy(desc('count')) \
            .limit(n) \
            .toPandas()
        
        return top_cats.to_dict('records')


# ========================================
# TIME ANALYSIS (from file 03)
# ========================================
class TimeAnalyzer:
    """
    Phân tích time patterns (hour, day of week, trending speed)
    """
    
    @staticmethod
    def analyze_publish_patterns(df: DataFrame) -> Dict:
        """
        Phân tích patterns về thời gian đăng video
        """
        logger.info("🕐 Analyzing publish time patterns...")
        
        # Hour distribution
        hour_dist = df.groupBy('publish_hour') \
            .agg(count('*').alias('video_count')) \
            .orderBy('publish_hour') \
            .toPandas()
        
        # Day of week distribution
        dow_dist = df.groupBy('publish_day_of_week') \
            .agg(count('*').alias('video_count')) \
            .orderBy('publish_day_of_week') \
            .toPandas()
        
        # Peak times
        peak_hour = hour_dist.loc[hour_dist['video_count'].idxmax(), 'publish_hour']
        peak_dow = dow_dist.loc[dow_dist['video_count'].idxmax(), 'publish_day_of_week']
        
        result = {
            'hour_distribution': hour_dist.to_dict('records'),
            'dow_distribution': dow_dist.to_dict('records'),
            'peak_hour': int(peak_hour),
            'peak_day': int(peak_dow),
            'total_videos': df.count()
        }
        
        logger.info(f"✅ Peak hour: {peak_hour}:00, Peak day: {peak_dow}")
        return result
    
    @staticmethod
    def analyze_trending_speed(df: DataFrame) -> Dict:
        """
        Phân tích tốc độ lọt trending (days_to_trending)
        """
        logger.info("⚡ Analyzing trending speed...")
        
        speed_stats = df.select(
            avg('days_to_trending').alias('avg_days'),
            spark_min('days_to_trending').alias('min_days'),
            spark_max('days_to_trending').alias('max_days')
        ).first()
        
        # Distribution of trending speed
        speed_dist = df.groupBy('days_to_trending') \
            .agg(count('*').alias('video_count')) \
            .orderBy('days_to_trending') \
            .limit(30) \
            .toPandas()
        
        result = {
            'avg_days_to_trending': float(speed_stats['avg_days']) if speed_stats['avg_days'] else 0,
            'min_days': int(speed_stats['min_days']) if speed_stats['min_days'] else 0,
            'max_days': int(speed_stats['max_days']) if speed_stats['max_days'] else 0,
            'distribution': speed_dist.to_dict('records')
        }
        
        logger.info(f"✅ Avg days to trending: {result['avg_days_to_trending']:.2f}")
        return result
    
    @staticmethod
    def compare_time_patterns(current_df: DataFrame, baseline: Dict) -> Tuple[Dict, List[Dict]]:
        """
        So sánh time patterns với baseline
        """
        if baseline is None:
            return None, []
        
        # Current hour distribution
        current_hourly = current_df.groupBy('publish_hour').count().toPandas()
        current_hourly_dict = dict(zip(current_hourly['publish_hour'], current_hourly['count']))
        
        # Baseline hour distribution
        baseline_hourly = baseline.get('statistics', {}).get('hourly_distribution', {})
        
        # Compare and detect anomalies
        anomalies = []
        for hour, curr_count in current_hourly_dict.items():
            base_count = baseline_hourly.get(str(hour), 0)
            
            if base_count > 0:
                change_ratio = ((curr_count - base_count) / base_count * 100)
                
                if abs(change_ratio) > 50:  # 50% threshold for time anomalies
                    anomalies.append({
                        'type': 'TIME_PATTERN',
                        'hour': hour,
                        'direction': '📈 SURGE' if change_ratio > 0 else '📉 DECLINE',
                        'change_ratio': change_ratio,
                        'current': curr_count,
                        'baseline': base_count
                    })
        
        return current_hourly_dict, anomalies


# ========================================
# INTERACTION ANALYSIS (from file 04)
# ========================================
class InteractionAnalyzer:
    """
    Phân tích engagement metrics (views, likes, comments, engagement rate)
    """
    
    @staticmethod
    def analyze_engagement_metrics(df: DataFrame) -> Dict:
        """
        Phân tích các metrics tương tác
        """
        logger.info("💝 Analyzing engagement metrics...")
        
        # Overall statistics
        stats = df.select(
            avg('view_count').alias('avg_views'),
            avg('likes').alias('avg_likes'),
            avg('comment_count').alias('avg_comments'),
            avg('engagement_rate').alias('avg_engagement'),
            spark_max('view_count').alias('max_views'),
            spark_max('engagement_rate').alias('max_engagement')
        ).first()
        
        # Engagement rate distribution by category
        by_category = df.groupBy('category_name') \
            .agg(
                avg('engagement_rate').alias('avg_engagement'),
                count('*').alias('video_count')
            ) \
            .orderBy(desc('avg_engagement')) \
            .toPandas()
        
        result = {
            'avg_views': float(stats['avg_views']) if stats['avg_views'] else 0,
            'avg_likes': float(stats['avg_likes']) if stats['avg_likes'] else 0,
            'avg_comments': float(stats['avg_comments']) if stats['avg_comments'] else 0,
            'avg_engagement_rate': float(stats['avg_engagement']) if stats['avg_engagement'] else 0,
            'max_views': int(stats['max_views']) if stats['max_views'] else 0,
            'max_engagement': float(stats['max_engagement']) if stats['max_engagement'] else 0,
            'by_category': by_category.to_dict('records')
        }
        
        logger.info(f"✅ Avg engagement rate: {result['avg_engagement_rate']:.4f}")
        return result
    
    @staticmethod
    def detect_viral_videos(df: DataFrame, viral_threshold: int = 10000000) -> List[Dict]:
        """
        Detect viral videos (views > threshold)
        """
        logger.info(f"🔥 Detecting viral videos (threshold: {viral_threshold:,})...")
        
        viral_df = df.filter(col('view_count') > viral_threshold) \
            .select('video_id', 'title', 'category_name', 'view_count', 'engagement_rate') \
            .orderBy(desc('view_count')) \
            .limit(20)
        
        viral_list = viral_df.toPandas().to_dict('records')
        
        logger.info(f"✅ Found {len(viral_list)} viral videos")
        return viral_list
    
    @staticmethod
    def compare_engagement(current_df: DataFrame, baseline: Dict) -> Tuple[Dict, List[Dict]]:
        """
        So sánh engagement metrics với baseline
        """
        if baseline is None:
            return None, []
        
        # Current stats
        curr_stats = current_df.select(
            avg('view_count').alias('avg_views'),
            avg('likes').alias('avg_likes'),
            avg('engagement_rate').alias('avg_engagement')
        ).first()
        
        curr_avg_views = float(curr_stats['avg_views']) if curr_stats['avg_views'] else 0
        curr_avg_likes = float(curr_stats['avg_likes']) if curr_stats['avg_likes'] else 0
        curr_engagement = float(curr_stats['avg_engagement']) if curr_stats['avg_engagement'] else 0
        
        # Baseline stats
        base_avg_views = baseline.get('statistics', {}).get('avg_views_per_video', 0)
        base_avg_likes = baseline.get('statistics', {}).get('avg_likes_per_video', 0)
        base_engagement = baseline.get('engagement_benchmarks', {}).get('avg_engagement_rate', 0)
        
        # Calculate changes
        anomalies = []
        
        # Engagement change
        if base_engagement > 0:
            eng_change = ((curr_engagement - base_engagement) / base_engagement * 100)
            
            if abs(eng_change) > 20:
                anomalies.append({
                    'type': 'ENGAGEMENT_OVERALL',
                    'direction': '📈 SURGE' if eng_change > 0 else '📉 DECLINE',
                    'change_ratio': eng_change,
                    'current': curr_engagement,
                    'baseline': base_engagement
                })
        
        # Detect viral videos
        viral_threshold = baseline.get('thresholds', {}).get('viral_view_threshold', 10000000)
        viral_videos = InteractionAnalyzer.detect_viral_videos(current_df, viral_threshold)
        
        for video in viral_videos:
            anomalies.append({
                'type': 'VIRAL_VIDEO',
                'video_title': video['title'],
                'views': video['view_count'],
                'category': video['category_name'],
                'engagement': video['engagement_rate']
            })
        
        comparison_data = {
            'current_avg_views': curr_avg_views,
            'baseline_avg_views': base_avg_views,
            'current_avg_likes': curr_avg_likes,
            'baseline_avg_likes': base_avg_likes,
            'current_engagement': curr_engagement,
            'baseline_engagement': base_engagement,
            'viral_count': len(viral_videos)
        }
        
        return comparison_data, anomalies
    
    @staticmethod
    def calculate_view_velocity(df: DataFrame) -> DataFrame:
        """
        Tính view velocity (views per minute since published)
        Quan trọng cho real-time prediction
        """
        from pyspark.sql.functions import unix_timestamp, current_timestamp
        
        df = df.withColumn(
            'minutes_since_publish',
            (unix_timestamp(current_timestamp()) - unix_timestamp(col('publishedAt'))) / 60
        )
        
        df = df.withColumn(
            'view_velocity',
            when(col('minutes_since_publish') > 0,
                 col('view_count') / col('minutes_since_publish')
            ).otherwise(0)
        )
        
        return df


# ========================================
# INTEGRATED ANALYZER
# ========================================
class YouTubeAnalyzer:
    """
    Main analyzer class that combines all analysis modules
    """
    
    def __init__(self):
        self.category_analyzer = CategoryAnalyzer()
        self.time_analyzer = TimeAnalyzer()
        self.interaction_analyzer = InteractionAnalyzer()
    
    def full_analysis(self, df: DataFrame, baselines: Dict = None) -> Dict:
        """
        Chạy full analysis trên DataFrame
        """
        logger.info("🚀 Running full analysis...")
        
        results = {
            'category': self.category_analyzer.analyze_category_distribution(df),
            'time': self.time_analyzer.analyze_publish_patterns(df),
            'engagement': self.interaction_analyzer.analyze_engagement_metrics(df),
            'trending_speed': self.time_analyzer.analyze_trending_speed(df)
        }
        
        # Compare with baselines if provided
        if baselines:
            results['anomalies'] = []
            
            if 'category' in baselines and baselines['category']:
                _, cat_anomalies = self.category_analyzer.compare_with_baseline(df, baselines['category'])
                results['anomalies'].extend(cat_anomalies)
            
            if 'time' in baselines and baselines['time']:
                _, time_anomalies = self.time_analyzer.compare_time_patterns(df, baselines['time'])
                results['anomalies'].extend(time_anomalies)
            
            if 'interaction' in baselines and baselines['interaction']:
                _, eng_anomalies = self.interaction_analyzer.compare_engagement(df, baselines['interaction'])
                results['anomalies'].extend(eng_anomalies)
        
        logger.info(f"✅ Analysis complete. Found {len(results.get('anomalies', []))} anomalies")
        return results


if __name__ == '__main__':
    print("✅ Analysis module loaded successfully")
    print("Available analyzers:")
    print("  - CategoryAnalyzer")
    print("  - TimeAnalyzer")
    print("  - InteractionAnalyzer")
    print("  - YouTubeAnalyzer (integrated)")