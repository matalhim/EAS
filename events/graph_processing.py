import math
import time
from datetime import datetime
from statistics import mean, median

import matplotlib.pyplot as plt
import numpy as np
from config import BATCH, check_time, dates_list, delta_time, runs_colllect
from pymongo import UpdateOne, errors
from tqdm import tqdm

from collections import defaultdict
import datetime


def hist_events_by_day(db):
    # Словарь для хранения количества событий по каждой дате
    event_counts = defaultdict(int)

    # Получаем список всех коллекций, которые начинаются с 'RUN_'
    collections = [col for col in db.list_collection_names()
                   if col.startswith("RUN_")]

    # Проходим по каждой коллекции и считаем события по дням
    for collection_name in collections:
        collection = db[collection_name]
        # Ищем все документы в коллекции, которые содержат поле 'date'
        cursor = collection.find({"date": {"$exists": True}})
        for document in cursor:
            date_str = document.get("date")
            if date_str:
                try:
                    # Преобразуем строку даты в объект datetime
                    date_obj = datetime.datetime.strptime(
                        date_str, "%Y-%m-%d").date()
                    # Увеличиваем счетчик событий для этой даты
                    event_counts[date_obj] += 1
                except ValueError:
                    print(f"Некорректный формат даты: {date_str}")

    # Сортируем даты и их количество событий
    dates = sorted(event_counts.keys())
    counts = [event_counts[date] for date in dates]

    # Построение гистограммы
    plt.figure(figsize=(12, 6))
    plt.bar(dates, counts, color='blue', alpha=0.7)
    plt.xlabel("День")
    plt.ylabel("Количество событий")
    plt.title("Гистограмма количества найденных совместных событий по дням")
    plt.xticks(dates, [date.strftime('%Y-%m-%d')
               for date in dates], rotation=90, ha='center')
    plt.tight_layout()
    plt.savefig('./events/plots/hist_events_by_day.png')
    plt.show()
