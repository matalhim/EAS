import os
from pymongo import MongoClient, errors

class DatabaseConnection:
    def __init__(self):
        self.mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
        self.db_name = os.environ.get('DB_NAME', 'test')
        self.client = None
        self.db = None
        self.connect()

    def connect(self):
        try:
            self.client = MongoClient(self.mongo_url, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
        except errors.ConnectionFailure as e:
            print(f"Ошибка подключения к базе данных: {e}")

    def get_db(self):
        return self.db
