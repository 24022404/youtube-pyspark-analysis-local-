"""
Flask Dashboard Backend
WebSocket + REST API để serve real-time data cho frontend
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from pymongo import MongoClient
from kafka import KafkaConsumer
import threading
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========================================
# FLASK APP SETUP
# ========================================
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'your-secret-key')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# ========================================
# MONGODB CONNECTION
# ========================================
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://admin:admin123@mongodb:27017/youtube_analytics?authSource=admin')
MONGO_DB = os.getenv('MONGO_DB_NAME', 'youtube_analytics')

try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    logger.info(f"✅ Connected to MongoDB: {MONGO_DB}")
except Exception as e:
    logger.error(f"❌ MongoDB connection failed: {e}")
    db = None

# ========================================
# KAFKA CONSUMER (for real-time updates)
# ========================================
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_ANALYTICS', 'analytics-results')

kafka_consumer = None

def start_kafka_consumer():
    """
    Start Kafka consumer in background thread
    """
    global kafka_consumer
    
    try:
        kafka_consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='dashboard-group'
        )
        
        logger.info(f"✅ Kafka consumer started: {KAFKA_TOPIC}")
        
        # Consume messages and emit via WebSocket
        for message in kafka_consumer:
            data = message.value
            socketio.emit('analytics_update', data, namespace='/', broadcast=True)
            
    except Exception as e:
        logger.error(f"❌ Kafka consumer error: {e}")

# ========================================
# REST API ENDPOINTS
# ========================================

@app.route('/')
def index():
    """
    Serve dashboard homepage
    """
    return jsonify({
        'status': 'ok',
        'message': 'YouTube Analytics Dashboard API',
        'endpoints': {
            '/api/stats': 'Get overall statistics',
            '/api/trending/latest': 'Get latest trending videos',
            '/api/trending/top': 'Get top trending videos',
            '/api/analytics/category': 'Get category distribution',
            '/api/analytics/time': 'Get time patterns',
            '/api/analytics/engagement': 'Get engagement metrics',
            '/api/predictions': 'Get tomorrow predictions'
        }
    })

@app.route('/api/stats')
def get_stats():
    """
    Get overall statistics
    """
    try:
        trending_col = db['trending_videos']
        
        # Total videos
        total_videos = trending_col.count_documents({})
        
        # Recent videos (last 1 hour)
        one_hour_ago = datetime.now() - timedelta(hours=1)
        recent_videos = trending_col.count_documents({
            'fetch_timestamp': {'$gte': one_hour_ago.isoformat()}
        })
        
        # Aggregate stats
        pipeline = [
            {
                '$group': {
                    '_id': None,
                    'total_views': {'$sum': '$view_count'},
                    'avg_views': {'$avg': '$view_count'},
                    'avg_engagement': {'$avg': '$engagement_rate'},
                    'max_views': {'$max': '$view_count'}
                }
            }
        ]
        
        stats = list(trending_col.aggregate(pipeline))
        
        result = {
            'total_videos': total_videos,
            'recent_videos': recent_videos,
            'total_views': stats[0]['total_views'] if stats else 0,
            'avg_views': stats[0]['avg_views'] if stats else 0,
            'avg_engagement': stats[0]['avg_engagement'] if stats else 0,
            'max_views': stats[0]['max_views'] if stats else 0,
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error in /api/stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/trending/latest')
def get_latest_trending():
    """
    Get latest trending videos
    """
    try:
        limit = int(request.args.get('limit', 20))
        
        trending_col = db['trending_videos']
        
        videos = list(trending_col.find(
            {},
            {
                'video_id': 1,
                'title': 1,
                'category_name': 1,
                'channelTitle': 1,
                'view_count': 1,
                'likes': 1,
                'engagement_rate': 1,
                'view_velocity': 1,
                'thumbnail_link': 1,
                'fetch_timestamp': 1,
                '_id': 0
            }
        ).sort('fetch_timestamp', -1).limit(limit))
        
        return jsonify({
            'count': len(videos),
            'videos': videos
        })
        
    except Exception as e:
        logger.error(f"Error in /api/trending/latest: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/trending/top')
def get_top_trending():
    """
    Get top trending videos by views
    """
    try:
        limit = int(request.args.get('limit', 10))
        
        trending_col = db['trending_videos']
        
        videos = list(trending_col.find(
            {},
            {
                'video_id': 1,
                'title': 1,
                'category_name': 1,
                'channelTitle': 1,
                'view_count': 1,
                'likes': 1,
                'engagement_rate': 1,
                'thumbnail_link': 1,
                '_id': 0
            }
        ).sort('view_count', -1).limit(limit))
        
        return jsonify({
            'count': len(videos),
            'videos': videos
        })
        
    except Exception as e:
        logger.error(f"Error in /api/trending/top: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/category')
def get_category_distribution():
    """
    Get category distribution
    """
    try:
        trending_col = db['trending_videos']
        
        pipeline = [
            {
                '$group': {
                    '_id': '$category_name',
                    'count': {'$sum': 1},
                    'total_views': {'$sum': '$view_count'},
                    'avg_views': {'$avg': '$view_count'},
                    'avg_engagement': {'$avg': '$engagement_rate'}
                }
            },
            {
                '$sort': {'count': -1}
            }
        ]
        
        categories = list(trending_col.aggregate(pipeline))
        
        # Format for frontend
        formatted = []
        for cat in categories:
            formatted.append({
                'category': cat['_id'],
                'count': cat['count'],
                'total_views': cat['total_views'],
                'avg_views': cat['avg_views'],
                'avg_engagement': cat['avg_engagement']
            })
        
        return jsonify({
            'count': len(formatted),
            'categories': formatted
        })
        
    except Exception as e:
        logger.error(f"Error in /api/analytics/category: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/time')
def get_time_patterns():
    """
    Get time patterns (hourly, daily)
    """
    try:
        trending_col = db['trending_videos']
        
        # Hourly distribution
        hourly_pipeline = [
            {
                '$group': {
                    '_id': '$publish_hour',
                    'count': {'$sum': 1},
                    'avg_views': {'$avg': '$view_count'}
                }
            },
            {
                '$sort': {'_id': 1}
            }
        ]
        
        hourly = list(trending_col.aggregate(hourly_pipeline))
        
        # Daily distribution
        daily_pipeline = [
            {
                '$group': {
                    '_id': '$publish_day_of_week',
                    'count': {'$sum': 1},
                    'avg_views': {'$avg': '$view_count'}
                }
            },
            {
                '$sort': {'_id': 1}
            }
        ]
        
        daily = list(trending_col.aggregate(daily_pipeline))
        
        return jsonify({
            'hourly': hourly,
            'daily': daily
        })
        
    except Exception as e:
        logger.error(f"Error in /api/analytics/time: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/engagement')
def get_engagement_metrics():
    """
    Get engagement metrics over time
    """
    try:
        analytics_col = db['analytics_snapshots']
        
        # Get latest snapshots
        snapshots = list(analytics_col.find(
            {},
            {
                'window_start': 1,
                'category_name': 1,
                'avg_views': 1,
                'avg_engagement': 1,
                'avg_view_velocity': 1,
                '_id': 0
            }
        ).sort('window_start', -1).limit(50))
        
        return jsonify({
            'count': len(snapshots),
            'snapshots': snapshots
        })
        
    except Exception as e:
        logger.error(f"Error in /api/analytics/engagement: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/predictions')
def get_predictions():
    """
    Get tomorrow's predictions
    """
    try:
        predictions_col = db['predictions']
        
        predictions = list(predictions_col.find(
            {},
            {
                'video_id': 1,
                'title': 1,
                'category_name': 1,
                'trending_score': 1,
                'predicted_views': 1,
                'current_view_count': 1,
                'view_velocity': 1,
                '_id': 0
            }
        ).sort('trending_score', -1).limit(20))
        
        return jsonify({
            'count': len(predictions),
            'predictions': predictions,
            'generated_at': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in /api/predictions: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/realtime')
def get_realtime_summary():
    """
    Get real-time summary (last 5 minutes)
    """
    try:
        five_min_ago = datetime.now() - timedelta(minutes=5)
        
        trending_col = db['trending_videos']
        
        recent = list(trending_col.find({
            'fetch_timestamp': {'$gte': five_min_ago.isoformat()}
        }))
        
        if not recent:
            return jsonify({
                'videos_count': 0,
                'message': 'No recent data'
            })
        
        # Calculate stats
        total_views = sum(v.get('view_count', 0) for v in recent)
        avg_engagement = sum(v.get('engagement_rate', 0) for v in recent) / len(recent)
        
        # Category breakdown
        categories = {}
        for v in recent:
            cat = v.get('category_name', 'Unknown')
            categories[cat] = categories.get(cat, 0) + 1
        
        return jsonify({
            'videos_count': len(recent),
            'total_views': total_views,
            'avg_engagement': avg_engagement,
            'categories': categories,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in /api/analytics/realtime: {e}")
        return jsonify({'error': str(e)}), 500

# ========================================
# WEBSOCKET EVENTS
# ========================================

@socketio.on('connect')
def handle_connect():
    """
    Handle client connection
    """
    logger.info(f"Client connected: {request.sid}")
    emit('connection_response', {'status': 'connected', 'sid': request.sid})

@socketio.on('disconnect')
def handle_disconnect():
    """
    Handle client disconnection
    """
    logger.info(f"Client disconnected: {request.sid}")

@socketio.on('request_update')
def handle_update_request():
    """
    Client requests data update
    """
    try:
        # Get latest stats
        stats = get_stats().get_json()
        emit('stats_update', stats)
        
        # Get latest videos
        videos = get_latest_trending().get_json()
        emit('videos_update', videos)
        
    except Exception as e:
        logger.error(f"Error handling update request: {e}")

# ========================================
# BACKGROUND TASKS
# ========================================

def emit_periodic_updates():
    """
    Emit updates every 30 seconds
    """
    while True:
        try:
            time.sleep(30)
            
            # Get realtime summary
            with app.app_context():
                summary = get_realtime_summary().get_json()
                socketio.emit('realtime_update', summary, namespace='/', broadcast=True)
                
        except Exception as e:
            logger.error(f"Error in periodic updates: {e}")

# ========================================
# MAIN
# ========================================

if __name__ == '__main__':
    logger.info("="*60)
    logger.info("YOUTUBE ANALYTICS DASHBOARD BACKEND")
    logger.info("="*60)
    logger.info(f"MongoDB: {MONGO_URI}")
    logger.info(f"Kafka: {KAFKA_SERVERS}")
    logger.info("="*60)
    
    # Start Kafka consumer in background
    kafka_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    kafka_thread.start()
    logger.info("✅ Kafka consumer thread started")
    
    # Start periodic updates
    update_thread = threading.Thread(target=emit_periodic_updates, daemon=True)
    update_thread.start()
    logger.info("✅ Periodic update thread started")
    
    # Run Flask app with SocketIO
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', 5000))
    
    logger.info(f"🚀 Starting server on {host}:{port}")
    socketio.run(app, host=host, port=port, debug=False, allow_unsafe_werkzeug=True)
