"""
09_api_server.py
Flask API Server - Cầu nối giữa MongoDB và Dashboard
"""

from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient
import json

app = Flask(__name__)
CORS(app)  # Cho phép dashboard truy cập

# Kết nối MongoDB
mongo_client = None
db = None

try:
    mongo_client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
    mongo_client.server_info()
    db = mongo_client['youtube_analytics']
    print("✅ Connected to MongoDB")
except Exception as e:
    print(f"❌ MongoDB error: {e}")
    db = None

@app.route('/api/realtime', methods=['GET'])
def get_realtime_data():
    """Lấy dữ liệu real-time từ MongoDB"""
    if db is None:  # ← SỬA: Thay 'if not db' thành 'if db is None'
        return jsonify({'error': 'MongoDB not connected'}), 500
    
    try:
        # Lấy 100 videos mới nhất
        data = list(db.realtime_data.find(
            {},
            {'_id': 0}  # Không trả về _id
        ).sort('processing_timestamp', -1).limit(100))
        
        print(f"📤 Sent {len(data)} videos to dashboard")
        return jsonify(data)
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/predictions', methods=['GET'])
def get_predictions():
    """Lấy predictions từ MongoDB"""
    if db is None:  # ← SỬA
        return jsonify({'error': 'MongoDB not connected'}), 500
    
    try:
        data = list(db.predictions.find(
            {},
            {'_id': 0}
        ).sort('predicted_views_tomorrow', -1).limit(10))
        
        return jsonify(data)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/category_stats', methods=['GET'])
def get_category_stats():
    """Lấy thống kê theo category"""
    if db is None:  # ← SỬA
        return jsonify({'error': 'MongoDB not connected'}), 500
    
    try:
        data = list(db.category_stats.find(
            {},
            {'_id': 0}
        ).sort('timestamp', -1).limit(20))
        
        return jsonify(data)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Kiểm tra API có hoạt động không"""
    return jsonify({
        'status': 'ok',
        'mongodb': 'connected' if db is not None else 'disconnected'  # ← SỬA
    })

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 FLASK API SERVER")
    print("=" * 60)
    print("📍 Running on: http://localhost:5000")
    print("📡 Endpoints:")
    print("   • /api/realtime - Real-time videos")
    print("   • /api/predictions - ML predictions")
    print("   • /api/category_stats - Category stats")
    print("   • /api/health - Health check")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=5000, debug=False)
