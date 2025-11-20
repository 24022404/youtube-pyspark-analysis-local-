# 📊 YOUTUBE ANALYTICS WITH PYSPARK

**Phân tích dữ liệu YouTube trending với PySpark, Kafka và Machine Learning**

## 👥 Nhóm thực hiện
| MSSV     | Họ tên            |
|----------|-------------------|
| 23020354 | Tôn Thành Đạt     |
| 23020370 | Đồng Mạnh Hùng    |
| 24022404 | Nguyễn Đức Minh   |
 
📝 [Báo cáo Latex](https://www.overleaf.com/3531892396cjspnhknhykc#a6aceb)

---

## 🎯 Mục tiêu
1. **Phân tích BATCH**: Phân tích pattern từ data lịch sử (2020-2024)
2. **Phân tích REAL-TIME**: Dự đoán trending videos trong thời gian thực
3. **Dashboard**: Hiển thị kết quả trực quan

---

## 🏗️ Kiến trúc hệ thống

```
📥 DATA COLLECTION
   │
   ├─▶ Kaggle (2020-2024) ─┐
   │                        ├─▶ raw_data.csv
   └─▶ YouTube Crawl ───────┘        │
                                     │
                                     ├─▶ 🧹 Preprocessing ──▶ 📊 Batch Analysis ──▶ 📈 Insights
                                     │
                                     └─▶ 🤖 Train ML Model ──────────────────┐
                                                                             │
⚡ REAL-TIME PIPELINE                                                        │
                                                                             ▼
[Crawl] ──▶ [Kafka] ──▶ [PySpark] ──▶ [Predict] ──▶ [MongoDB] ──▶ 🌐 Dashboard
 YouTube      Topic      + Model        Views/Likes
```

---

## 📂 Cấu trúc project

```
├── 00_download_kaggle_data.ipynb     # Download Kaggle dataset
├── 01_crawl_youtube_data.ipynb       # Crawl YouTube API
├── 02_preprocessing.ipynb            # Clean & transform data
├── 03_analysis_category.ipynb        # Phân tích theo category
├── 04_analysis_time.ipynb            # Phân tích theo thời gian
├── 05_analysis_engagement.ipynb      # Phân tích engagement
├── 06_ml_prediction.ipynb            # Train GBT model
├── 07_kafka_producer.py              # Gửi data lên Kafka
├── 08_realtime_analysis.py           # PySpark streaming + ML
├── 09_api_server.py                  # Flask API
├── dashboard.html                    # Web dashboard
└── clear_mongodb.py                  # Clear MongoDB (nếu cần)
```

---

## 🚀 Cài đặt & Chạy

### Yêu cầu
- Kafka
- MongoDB

### Phân tích tĩnh (Batch)
```bash
# Chạy lần lượt:
00_download_kaggle_data.ipynb
01_crawl_youtube_data.ipynb
02_preprocessing.ipynb
03_analysis_category.ipynb
04_analysis_time.ipynb
05_analysis_engagement.ipynb
```

### Phân tích thời gian thực (Real-time)
```bash
# Chạy file 00_download_kaggle_data.ipynb, 02_preprocessing.ipynb trước
# Rồi chạy lần lượt 5 terminal
# Terminal 1: Zookeeper
cd C:\kafka\kafka_2.13-3.3.2
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

# Terminal 2: Kafka
cd C:\kafka\kafka_2.13-3.3.2
bin\windows\kafka-server-start.bat config\server.properties

# Terminal 3: Producer
python 07_kafka_producer.py

# Terminal 4: Consumer
python 08_realtime_analysis.py

# Terminal 5: API Server
python 09_api_server.py
```

Mở dashboard: `http://127.0.0.1:5500/08_dashboard.html`

---

## 📊 Kết quả

### Batch Analysis
- ✅ Top 10 categories phổ biến nhất
- ✅ Giờ/ngày đăng video trending nhiều nhất
- ✅ Tương quan giữa views/likes/comments

### Real-time Analysis
- ✅ Dự đoán views/likes cho videos mới (RMSE, MAE, R²)
- ✅ Phát hiện trending patterns theo thời gian thực
- ✅ Dashboard cập nhật tự động mỗi 10s

---

## 📌 Notes
- Data crawl giới hạn 50 videos/ngày (YouTube API quota)
- Model GBT được train trên ~200K videos
- Dashboard hiển thị realtime với confidence score

---

## 📞 Liên hệ
- Tôn Thành Đạt: 23020354@vnu.edu.vn
- Đồng Mạnh Hùng: 23020370@vnu.edu.vn
- Nguyễn Đức Minh: 24022404@vnu.edu.vn