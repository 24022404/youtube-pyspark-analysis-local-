from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['youtube_analytics']

# Xóa data
db.realtime_data.delete_many({})
db.predictions.delete_many({})

print("✅ Đã xóa data cũ!")
client.close()