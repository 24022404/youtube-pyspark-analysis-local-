"""
Kafka Producer
Nâng cấp từ file 05_integrated_realtime_monitoring.ipynb
Fetch YouTube API → Push to Kafka topic
"""
import os
import sys
import json
import time
from datetime import datetime
from googleapiclient.discovery import build
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Import config
sys.path.append(os.path.dirname(__file__))
from config import YouTubeConfig, KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class YouTubeKafkaProducer:
    """
    Fetch YouTube trending videos và push vào Kafka
    """
    
    def __init__(self):
        self.youtube_api = None
        self.kafka_producer = None
        self.category_map = YouTubeConfig.CATEGORY_MAP
        self.fetch_interval = YouTubeConfig.FETCH_INTERVAL
        
        self._init_youtube_api()
        self._init_kafka_producer()
    
    def _init_youtube_api(self):
        """
        Initialize YouTube API client
        """
        try:
            api_key = YouTubeConfig.API_KEY
            if not api_key:
                raise ValueError("YouTube API key not found in environment")
            
            self.youtube_api = build('youtube', 'v3', developerKey=api_key)
            logger.info("✅ YouTube API initialized")
        except Exception as e:
            logger.error(f"❌ YouTube API initialization failed: {e}")
            raise
    
    def _init_kafka_producer(self):
        """
        Initialize Kafka producer
        """
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"✅ Kafka producer initialized: {KafkaConfig.BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"❌ Kafka producer initialization failed: {e}")
            raise
    
    def fetch_trending_videos(self, region_code='US', max_results=50):
        """
        Fetch trending videos từ YouTube API
        Tái sử dụng logic từ file 05
        """
        try:
            logger.info(f"📡 Fetching trending videos (region: {region_code}, max: {max_results})...")
            
            request = self.youtube_api.videos().list(
                part='snippet,statistics',
                chart='mostPopular',
                regionCode=region_code,
                maxResults=max_results
            )
            response = request.execute()
            
            videos = []
            for item in response.get('items', []):
                video_data = self._parse_video_item(item)
                if video_data:
                    videos.append(video_data)
            
            logger.info(f"✅ Fetched {len(videos)} trending videos")
            return videos
            
        except Exception as e:
            logger.error(f"❌ Error fetching trending videos: {e}")
            return []
    
    def _parse_video_item(self, item):
        """
        Parse video item từ YouTube API response
        """
        try:
            snippet = item.get('snippet', {})
            statistics = item.get('statistics', {})
            
            # Parse basic info
            video_data = {
                'video_id': item.get('id', ''),
                'title': snippet.get('title', ''),
                'publishedAt': snippet.get('publishedAt', ''),
                'channelId': snippet.get('channelId', ''),
                'channelTitle': snippet.get('channelTitle', ''),
                'categoryId': snippet.get('categoryId', ''),
                'description': snippet.get('description', ''),
                'tags': '|'.join(snippet.get('tags', [])),
                'thumbnail_link': snippet.get('thumbnails', {}).get('high', {}).get('url', ''),
                
                # Statistics
                'view_count': int(statistics.get('viewCount', 0)),
                'likes': int(statistics.get('likeCount', 0)),
                'dislikes': int(statistics.get('dislikeCount', 0)),
                'comment_count': int(statistics.get('commentCount', 0)),
                
                # Metadata
                'comments_disabled': statistics.get('commentCount') is None,
                'ratings_disabled': statistics.get('likeCount') is None,
                
                # Add timestamp
                'fetch_timestamp': datetime.now().isoformat(),
                'trending_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Add category name
            video_data['category_name'] = self.category_map.get(
                video_data['categoryId'], 
                'Unknown'
            )
            
            # Calculate engagement rate
            if video_data['view_count'] > 0:
                video_data['engagement_rate'] = (
                    video_data['likes'] + video_data['comment_count']
                ) / video_data['view_count']
            else:
                video_data['engagement_rate'] = 0.0
            
            return video_data
            
        except Exception as e:
            logger.error(f"❌ Error parsing video item: {e}")
            return None
    
    def send_to_kafka(self, video_data):
        """
        Send một video data vào Kafka topic
        """
        try:
            # Use video_id as key for partitioning
            key = video_data['video_id']
            
            # Send to Kafka
            future = self.kafka_producer.send(
                KafkaConfig.TOPIC_TRENDING,
                key=key,
                value=video_data
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"✅ Sent video {key} to topic {record_metadata.topic} "
                f"partition {record_metadata.partition} offset {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"❌ Kafka error sending video {video_data['video_id']}: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error sending to Kafka: {e}")
            return False
    
    def send_batch_to_kafka(self, videos):
        """
        Send một batch videos vào Kafka
        """
        success_count = 0
        fail_count = 0
        
        logger.info(f"📤 Sending {len(videos)} videos to Kafka...")
        
        for video in videos:
            if self.send_to_kafka(video):
                success_count += 1
            else:
                fail_count += 1
        
        # Flush to ensure all messages are sent
        self.kafka_producer.flush()
        
        logger.info(f"✅ Batch sent: {success_count} success, {fail_count} failed")
        return success_count, fail_count
    
    def run_continuous(self, region_code='US', max_results=50):
        """
        Chạy continuously - fetch và send mỗi N giây
        """
        logger.info(f"🚀 Starting continuous producer (interval: {self.fetch_interval}s)")
        logger.info(f"   Region: {region_code}, Max results: {max_results}")
        logger.info(f"   Kafka topic: {KafkaConfig.TOPIC_TRENDING}")
        logger.info("   Press Ctrl+C to stop")
        
        iteration = 0
        
        try:
            while True:
                iteration += 1
                start_time = time.time()
                
                logger.info(f"\n{'='*60}")
                logger.info(f"🔄 Iteration #{iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'='*60}")
                
                # Fetch videos
                videos = self.fetch_trending_videos(region_code, max_results)
                
                if videos:
                    # Send to Kafka
                    success, failed = self.send_batch_to_kafka(videos)
                    
                    # Stats
                    logger.info(f"📊 Stats: {success}/{len(videos)} sent successfully")
                    if failed > 0:
                        logger.warning(f"⚠️  {failed} videos failed to send")
                else:
                    logger.warning("⚠️  No videos fetched in this iteration")
                
                # Calculate sleep time
                elapsed = time.time() - start_time
                sleep_time = max(0, self.fetch_interval - elapsed)
                
                logger.info(f"⏱️  Iteration took {elapsed:.2f}s, sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Received stop signal, shutting down...")
            self.close()
        except Exception as e:
            logger.error(f"\n❌ Fatal error: {e}")
            self.close()
            raise
    
    def run_once(self, region_code='US', max_results=50):
        """
        Chạy một lần duy nhất (cho testing)
        """
        logger.info(f"🚀 Running once (region: {region_code}, max: {max_results})")
        
        videos = self.fetch_trending_videos(region_code, max_results)
        
        if videos:
            success, failed = self.send_batch_to_kafka(videos)
            logger.info(f"✅ Done: {success} success, {failed} failed")
        else:
            logger.warning("⚠️  No videos fetched")
        
        self.close()
    
    def close(self):
        """
        Close connections
        """
        logger.info("🔌 Closing connections...")
        
        if self.kafka_producer:
            self.kafka_producer.flush()
            self.kafka_producer.close()
            logger.info("✅ Kafka producer closed")
        
        logger.info("✅ Cleanup complete")


# ========================================
# TESTING FUNCTION
# ========================================
def test_youtube_api():
    """
    Test YouTube API connection
    """
    try:
        logger.info("🧪 Testing YouTube API...")
        youtube = build('youtube', 'v3', developerKey=YouTubeConfig.API_KEY)
        
        request = youtube.videos().list(
            part='snippet',
            chart='mostPopular',
            regionCode='US',
            maxResults=1
        )
        response = request.execute()
        
        if response.get('items'):
            logger.info("✅ YouTube API test successful")
            return True
        else:
            logger.error("❌ YouTube API test failed: No items returned")
            return False
            
    except Exception as e:
        logger.error(f"❌ YouTube API test failed: {e}")
        return False


def test_kafka_connection():
    """
    Test Kafka connection
    """
    try:
        logger.info("🧪 Testing Kafka connection...")
        producer = KafkaProducer(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Send test message
        test_data = {
            'test': True,
            'timestamp': datetime.now().isoformat()
        }
        
        future = producer.send(KafkaConfig.TOPIC_TRENDING, value=test_data)
        record_metadata = future.get(timeout=10)
        
        producer.close()
        
        logger.info(f"✅ Kafka test successful (topic: {record_metadata.topic})")
        return True
        
    except Exception as e:
        logger.error(f"❌ Kafka test failed: {e}")
        return False


# ========================================
# MAIN
# ========================================
def main():
    """
    Main entry point
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='YouTube Kafka Producer')
    parser.add_argument('--mode', choices=['continuous', 'once', 'test'], 
                        default='continuous',
                        help='Run mode: continuous, once, or test')
    parser.add_argument('--region', default='US', 
                        help='YouTube region code (default: US)')
    parser.add_argument('--max-results', type=int, default=50,
                        help='Max videos to fetch (default: 50)')
    parser.add_argument('--interval', type=int, default=None,
                        help='Fetch interval in seconds (default: from config)')
    
    args = parser.parse_args()
    
    # Print config
    logger.info("="*60)
    logger.info("YOUTUBE KAFKA PRODUCER")
    logger.info("="*60)
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Region: {args.region}")
    logger.info(f"Max results: {args.max_results}")
    logger.info(f"Kafka servers: {KafkaConfig.BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka topic: {KafkaConfig.TOPIC_TRENDING}")
    logger.info("="*60)
    
    # Test mode
    if args.mode == 'test':
        logger.info("\n🧪 Running tests...\n")
        
        youtube_ok = test_youtube_api()
        kafka_ok = test_kafka_connection()
        
        logger.info("\n" + "="*60)
        logger.info("TEST RESULTS")
        logger.info("="*60)
        logger.info(f"YouTube API: {'✅ PASS' if youtube_ok else '❌ FAIL'}")
        logger.info(f"Kafka: {'✅ PASS' if kafka_ok else '❌ FAIL'}")
        logger.info("="*60)
        
        if youtube_ok and kafka_ok:
            logger.info("\n✅ All tests passed! Ready to run producer.")
            sys.exit(0)
        else:
            logger.error("\n❌ Some tests failed. Please fix configuration.")
            sys.exit(1)
    
    # Create producer
    try:
        producer = YouTubeKafkaProducer()
        
        # Override fetch interval if specified
        if args.interval:
            producer.fetch_interval = args.interval
            logger.info(f"⚙️  Fetch interval overridden: {args.interval}s")
        
        # Run
        if args.mode == 'continuous':
            producer.run_continuous(args.region, args.max_results)
        elif args.mode == 'once':
            producer.run_once(args.region, args.max_results)
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
