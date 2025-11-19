"""
06_kafka_producer.py
Kafka Producer - Stream YouTube trending data vào Kafka topic
"""

from kafka import KafkaProducer
import json
import time
import pandas as pd
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YouTubeKafkaProducer:
    def __init__(self, 
                 bootstrap_servers='localhost:9092',
                 topic='youtube-trending',
                 csv_file='./data/raw_data.csv'):
        """
        Khởi tạo Kafka Producer
        
        Args:
            bootstrap_servers: Kafka server address
            topic: Kafka topic name
            csv_file: Đường dẫn file CSV chứa data
        """
        self.topic = topic
        self.csv_file = csv_file
        
        # Khởi tạo Kafka Producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Đảm bảo message được ghi thành công
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"✅ Kafka Producer đã kết nối đến {bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Không thể kết nối Kafka: {e}")
            raise
    
    def load_data(self):
        """Load data từ CSV"""
        try:
            df = pd.read_csv(self.csv_file)
            logger.info(f"📊 Đã load {len(df):,} videos từ {self.csv_file}")
            return df
        except Exception as e:
            logger.error(f"❌ Lỗi khi đọc CSV: {e}")
            raise
    
    def prepare_message(self, row):
        """
        Chuẩn bị message để gửi vào Kafka
        
        Args:
            row: Pandas Series (1 dòng từ DataFrame)
        Returns:
            dict: Message dạng JSON
        """
        return {
            'video_id': str(row['video_id']),
            'title': str(row['title']),
            'publishedAt': str(row['publishedAt']),
            'channelId': str(row['channelId']),
            'channelTitle': str(row['channelTitle']),
            'categoryId': int(row['categoryId']),
            'trending_date': str(row['trending_date']),
            'tags': str(row['tags']) if pd.notna(row['tags']) else '',
            'view_count': int(row['view_count']),
            'likes': int(row['likes']),
            'dislikes': int(row['dislikes']),
            'comment_count': int(row['comment_count']),
            'thumbnail_link': str(row['thumbnail_link']),
            'comments_disabled': bool(row['comments_disabled']),
            'ratings_disabled': bool(row['ratings_disabled']),
            'description': str(row['description']) if pd.notna(row['description']) else '',
            'kafka_timestamp': datetime.now().isoformat()
        }
    
    def stream_data(self, batch_size=10, delay=2):
        """
        Stream data theo batch vào Kafka
        
        Args:
            batch_size: Số message mỗi batch
            delay: Thời gian delay giữa các batch (giây)
        """
        df = self.load_data()
        total_records = len(df)
        sent_count = 0
        
        logger.info(f"🚀 Bắt đầu streaming {total_records:,} videos...")
        logger.info(f"   Batch size: {batch_size}")
        logger.info(f"   Delay: {delay}s")
        
        try:
            for idx, row in df.iterrows():
                # Chuẩn bị message
                message = self.prepare_message(row)
                
                # Gửi vào Kafka
                future = self.producer.send(self.topic, value=message)
                
                # Đợi xác nhận (tùy chọn)
                try:
                    record_metadata = future.get(timeout=10)
                    sent_count += 1
                    
                    # Log mỗi batch
                    if sent_count % batch_size == 0:
                        logger.info(
                            f"📤 Đã gửi {sent_count}/{total_records} videos "
                            f"({sent_count/total_records*100:.1f}%) | "
                            f"Topic: {record_metadata.topic} | "
                            f"Partition: {record_metadata.partition}"
                        )
                        time.sleep(delay)
                
                except Exception as e:
                    logger.error(f"❌ Lỗi khi gửi video {message['video_id']}: {e}")
            
            # Đảm bảo tất cả message được gửi
            self.producer.flush()
            logger.info(f"✅ HOÀN THÀNH! Đã gửi {sent_count}/{total_records} videos")
            
        except KeyboardInterrupt:
            logger.warning(f"⚠️ Dừng streaming! Đã gửi {sent_count}/{total_records} videos")
        except Exception as e:
            logger.error(f"❌ Lỗi khi streaming: {e}")
        finally:
            self.close()
    
    def stream_realtime_simulation(self, interval=5):
        """
        Giả lập streaming real-time (gửi data mới nhất liên tục)
        
        Args:
            interval: Thời gian delay giữa các message (giây)
        """
        df = self.load_data()
        
        # Lấy 50 videos mới nhất
        df_recent = df.tail(50)
        
        logger.info(f"🔄 Bắt đầu real-time simulation với {len(df_recent)} videos")
        logger.info(f"   Interval: {interval}s")
        
        try:
            count = 0
            while True:
                # Lặp qua các video
                for idx, row in df_recent.iterrows():
                    message = self.prepare_message(row)
                    
                    # Update timestamp mới
                    message['kafka_timestamp'] = datetime.now().isoformat()
                    
                    # Gửi vào Kafka
                    self.producer.send(self.topic, value=message)
                    count += 1
                    
                    logger.info(
                        f"📡 [{count}] Sent: {message['title'][:50]}... | "
                        f"Views: {message['view_count']:,}"
                    )
                    
                    time.sleep(interval)
        
        except KeyboardInterrupt:
            logger.warning(f"⚠️ Dừng simulation! Đã gửi {count} messages")
        finally:
            self.close()
    
    def close(self):
        """Đóng Kafka Producer"""
        if self.producer:
            self.producer.close()
            logger.info("👋 Đã đóng Kafka Producer")


def main():
    """Main function"""
    print("=" * 60)
    print("KAFKA PRODUCER - YOUTUBE TRENDING DATA")
    print("=" * 60)
    
    # Khởi tạo producer
    producer = YouTubeKafkaProducer(
        bootstrap_servers='localhost:9092',
        topic='youtube-trending',
        csv_file='./data/raw_data.csv'
    )
    
    # Menu lựa chọn
    print("\n📋 Chọn chế độ streaming:")
    print("   1. Batch streaming (gửi tất cả data)")
    print("   2. Real-time simulation (lặp liên tục)")
    
    choice = input("\nNhập lựa chọn (1 hoặc 2): ").strip()
    
    if choice == '1':
        batch_size = int(input("Batch size (mặc định 10): ") or 10)
        delay = float(input("Delay giữa các batch (giây, mặc định 2): ") or 2)
        producer.stream_data(batch_size=batch_size, delay=delay)
    
    elif choice == '2':
        interval = float(input("Interval giữa messages (giây, mặc định 5): ") or 5)
        producer.stream_realtime_simulation(interval=interval)
    
    else:
        print("❌ Lựa chọn không hợp lệ!")


if __name__ == "__main__":
    main()
