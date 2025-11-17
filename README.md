Xem phân công ở đây:  
https://docs.google.com/document/d/1nSEQl-qYVe-5RHFcNlCl-bvaru_Kta8JkdfGqhvIILg/edit?hl=vi&tab=t.0  

Báo cáo Latex ở đây:
https://www.overleaf.com/3531892396cjspnhknhykc#a6aceb  

# 📊 YouTube Real-time Analytics System

Hệ thống phân tích YouTube trending videos theo thời gian thực sử dụng **Kafka + PySpark + MongoDB + Flask + Chart.js**

## 🎯 Tính năng

✅ **Real-time Streaming**: Fetch YouTube API → Kafka → Spark Streaming → MongoDB  
✅ **Phân tích đa chiều**: Category, Time Patterns, Engagement Metrics  
✅ **Dự đoán AI**: Random Forest + Prophet dự đoán trending videos ngày mai  
✅ **Dashboard Live**: WebSocket real-time updates với Chart.js  
✅ **Scalable Architecture**: Docker Compose orchestration  

## 🏗️ Kiến trúc

```
YouTube API → Kafka Producer → Kafka Broker
                                    ↓
                            PySpark Streaming
                            (Preprocessing + Analysis + Prediction)
                                    ↓
                                MongoDB
                                    ↓
                            Flask Backend (WebSocket)
                                    ↓
                            Frontend Dashboard (Chart.js)
```

## 📂 Cấu trúc Project

```
youtube-realtime-analytics/
├── docker-compose.yml          # Orchestrate toàn bộ services
├── .env                        # Environment variables
├── data/
│   └── baselines/              # Baseline data từ file 01-04
├── notebooks/                  # Các file notebook gốc (01-05)
├── src/                        # Source code chính
│   ├── config.py              # Configuration module
│   ├── preprocessing.py       # Data preprocessing (từ file 01)
│   ├── analysis.py            # Analysis functions (từ file 02-04)
│   ├── kafka_producer.py      # YouTube API → Kafka (từ file 05)
│   ├── spark_streaming.py     # Spark Structured Streaming
│   └── prediction_model.py    # ML models (Random Forest + Prophet)
└── dashboard/
    ├── backend/
    │   └── app.py             # Flask + WebSocket backend
    └── frontend/
        ├── index.html         # Dashboard UI
        ├── styles.css         # Styling
        └── charts.js          # Chart.js logic
```

## 🚀 Cách chạy

### Bước 1: Chuẩn bị

```bash
# Clone hoặc tạo project directory
cd youtube-realtime-analytics

# Tạo .env file và điền YouTube API Key
cp .env.example .env
# Sửa YOUTUBE_API_KEY trong .env
```

### Bước 2: Start tất cả services

```bash
# Start toàn bộ hệ thống
docker-compose up -d

# Check logs
docker-compose logs -f
```

### Bước 3: Truy cập Dashboard

Mở trình duyệt: **http://localhost**

## 📋 Services

| Service | Port | Mô tả |
|---------|------|-------|
| Zookeeper | 2181 | Kafka coordination |
| Kafka | 9092, 9093 | Message broker |
| MongoDB | 27017 | Database (admin/admin123) |
| Spark Master | 8080 | Spark Web UI |
| Spark Worker | 8081 | Worker Web UI |
| Flask Backend | 5000 | REST API + WebSocket |
| Nginx Frontend | 80 | Dashboard |

## 🔧 Cấu hình

### YouTube API Key

1. Vào [Google Cloud Console](https://console.cloud.google.com/)
2. Tạo project mới
3. Enable YouTube Data API v3
4. Tạo API Key
5. Copy vào `.env`:

```bash
YOUTUBE_API_KEY=your_api_key_here
```

### MongoDB

Default credentials:
- Username: `admin`
- Password: `admin123`
- Database: `youtube_analytics`

### Kafka Topics

- `youtube-trending`: Raw trending videos
- `analytics-results`: Processed analytics

## 📊 Collections trong MongoDB

1. **trending_videos**: Raw trending videos với features
2. **analytics_snapshots**: Windowed analytics (1-minute tumbling)
3. **predictions**: Dự đoán videos trending ngày mai

## 🤖 Machine Learning Models

### Random Forest Classifier
- **Target**: `will_trend_tomorrow` (0/1)
- **Features**: 10 features (view_velocity, engagement_rate, etc.)
- **Metrics**: AUC, Accuracy

### Random Forest Regressor
- **Target**: `predicted_views_24h`
- **Metrics**: RMSE, R²

### Prophet (Optional)
- Time series forecasting cho view patterns

## 📈 Dashboard Features

1. **Header Stats**: Total videos, Recent videos, Avg engagement
2. **Category Distribution**: Pie chart
3. **Time Patterns**: Bar chart (hourly uploads)
4. **Engagement Rate**: Line chart (real-time updates)
5. **Top Trending**: Top 10 videos
6. **Predictions**: Tomorrow's potential trending videos
7. **Real-time Feed**: Last 5 minutes activity

## 🧪 Testing

### Test Kafka Producer

```bash
docker exec -it kafka-producer python kafka_producer.py --mode test
```

### Test Spark Streaming

```bash
docker exec -it spark-streaming python spark_streaming.py --test-mongo
```

### Test Backend API

```bash
curl http://localhost:5000/api/stats
```

## 🛠️ Development

### Run producer locally

```bash
cd src
pip install -r requirements-producer.txt
python kafka_producer.py --mode once
```

### Train models locally

```bash
cd src
pip install -r requirements-streaming.txt
python prediction_model.py --mode train
```

## 📝 Logs

```bash
# View all logs
docker-compose logs -f

# View specific service
docker-compose logs -f kafka-producer
docker-compose logs -f spark-streaming
docker-compose logs -f dashboard-backend
```

## 🐛 Troubleshooting

### Kafka không connect được?

```bash
# Restart Kafka
docker-compose restart kafka zookeeper
```

### MongoDB không connect được?

```bash
# Check MongoDB logs
docker-compose logs mongodb

# Restart MongoDB
docker-compose restart mongodb
```

### Dashboard không hiện data?

1. Check backend logs: `docker-compose logs dashboard-backend`
2. Check MongoDB có data: 
   ```bash
   docker exec -it mongodb mongosh -u admin -p admin123
   use youtube_analytics
   db.trending_videos.count()
   ```

### Spark job không chạy?

```bash
# Check Spark logs
docker-compose logs spark-streaming

# Restart Spark
docker-compose restart spark-master spark-worker-1 spark-streaming
```

## 🔥 Stop Services

```bash
# Stop all
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## 📚 Tham khảo

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [PySpark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [YouTube Data API](https://developers.google.com/youtube/v3)
- [Chart.js](https://www.chartjs.org/)
- [Flask-SocketIO](https://flask-socketio.readthedocs.io/)

## 👥 Credits

Được phát triển dựa trên các file 01-05 (preprocessing, category analysis, time analysis, interaction analysis, real-time monitoring).

**Tái sử dụng 100% logic từ các file gốc, nâng cấp lên kiến trúc real-time streaming!**

## 📄 License

MIT License