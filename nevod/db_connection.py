import os
from pymongo import MongoClient, errors

class DatabaseConnection:
    def __init__(self, mongo_url=None):
        self.mongo_url = mongo_url or os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
        self.client = None
        self.databases = {}
        self.connect()

    def connect(self):
        try:
            # Создание клиента MongoDB
            self.client = MongoClient(self.mongo_url, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
        except errors.ConnectionFailure as e:
            print(f"Ошибка подключения к MongoDB серверу: {e}")
            self.client = None

    def add_database(self, alias, db_name):
        if self.client:
            self.databases[alias] = self.client[db_name]
        else:
            print(f"Подключение к серверу MongoDB не установлено. Невозможно добавить базу данных: {db_name}")

    def get_database(self, alias):
        return self.databases.get(alias, None)