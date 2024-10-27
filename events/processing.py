from tqdm import tqdm
from pymongo import UpdateOne, errors
import math
import time
import os
import json
import re
from datetime import datetime
from statistics import mean, median
from config import (runs_colllect, delta_time, check_time, BATCH, dates_list)


def load_2_mongo(db, folder_path):

    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Папка не найдена: {folder_path}")

    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Папка не найдена: {folder_path}")

    run_pattern = re.compile(r'RUN_(\d+)_events')

    # Проходим по всем JSON-файлам в папке
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".json"):
            match = run_pattern.search(file_name)
            if match:
                run_number = match.group(1)
                collection_name = f"RUN_{run_number}_events"
                collection = db[collection_name]
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    try:
                        data = json.load(file)

                        def remove_id_field(data):
                            if isinstance(data, dict):
                                data.pop("_id", None)
                                for key, value in data.items():
                                    remove_id_field(value)
                            elif isinstance(data, list):
                                for item in data:
                                    remove_id_field(item)
                        remove_id_field(data)
                        if isinstance(data, list):
                            collection.insert_many(data)
                        else:
                            collection.insert_one(data)
                        print(f"Файл {file_name} успешно записан в коллекцию {
                            collection_name}.")
                    except json.JSONDecodeError:
                        print(f"Ошибка чтения файла {
                            file_name}: некорректный формат JSON.")
                    except Exception as e:
                        print(f"Ошибка записи файла {file_name}: {str(e)}")

    print("Все данные успешно записаны в базу данных.")
