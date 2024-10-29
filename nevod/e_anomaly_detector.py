import json as json

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from config import (
    BATCH,
    check_time,
    dates_list,
    decor_db,
    delta_time,
    neas_db,
    result_db,
    runs_colllect,
)
from db_connection import DatabaseConnection
from pymongo import MongoClient, UpdateOne, errors
from scipy.ndimage import uniform_filter1d
from sklearn.ensemble import IsolationForest


def detect_anomalies(df, window_size=45, threshold_ratio=0.97):
    """
    Модель для обнаружения аномалий на основе данных событий.
    Возвращает список интервалов аномалий и временные диапазоны в наносекундах.
    """
    df['time_minutes'] = df['time_ns'] / 1e9 / 60
    df_agg = df.groupby(df['time_minutes'].astype(
        int)).size().reset_index(name='event_count')

    df_agg['smoothed_event_count'] = uniform_filter1d(
        df_agg['event_count'], size=window_size)

    threshold = df_agg['smoothed_event_count'].mean() * threshold_ratio

    df_agg['anomaly'] = df_agg['smoothed_event_count'] < threshold

    anomalies = df_agg[df_agg['anomaly'] == True]

    intervals = []
    nanosecond_intervals = []
    current_interval = [None, None]
    first_event_ns = None
    last_event_ns = None

    for idx in range(len(anomalies)):
        minute_value = anomalies.iloc[idx]['time_minutes']

        matching_rows = df[df['time_minutes'].astype(int) == int(minute_value)]

        if not matching_rows.empty:
            if current_interval[0] is None:
                current_interval[0] = minute_value
                current_interval[1] = minute_value
                first_event_ns = matching_rows['time_ns'].min()
                last_event_ns = matching_rows['time_ns'].max()
            else:

                if minute_value - current_interval[1] <= 2:
                    current_interval[1] = minute_value
                    last_event_ns = matching_rows['time_ns'].max()
                else:
                    intervals.append(tuple(current_interval))
                    nanosecond_intervals.append(
                        (first_event_ns, last_event_ns))

                    current_interval = [minute_value, minute_value]
                    first_event_ns = matching_rows['time_ns'].min()
                    last_event_ns = matching_rows['time_ns'].max()

    if current_interval[0] is not None:
        intervals.append(tuple(current_interval))
        nanosecond_intervals.append((first_event_ns, last_event_ns))

    return intervals, nanosecond_intervals


def plot_histogram(df, intervals, bins, date):
    """
    Функция для построения гистограммы с выделением аномальных интервалов.
    """
    hist, bin_edges = np.histogram(df['time_minutes'], bins=bins)
    colors = ['blue'] * len(hist)

    for interval in intervals:
        for i in range(len(bin_edges) - 1):
            if interval[0] <= bin_edges[i + 1] and interval[1] >= bin_edges[i]:
                colors[i] = 'red'

    anomalous_hist = [hist[i] if colors[i] ==
                      'red' else 0 for i in range(len(hist))]
    normal_hist = [hist[i] if colors[i] ==
                   'blue' else 0 for i in range(len(hist))]

    plt.figure(figsize=(14, 4))

    plt.bar(bin_edges[:-1], normal_hist, color='black', alpha=0.7, width=1)

    plt.bar(bin_edges[:-1], anomalous_hist, color='red',
            alpha=0.5, width=1)

    plt.xlabel('время регистрации (минуты)')
    plt.ylabel('количество событий')
    plt.title(f'{date}: гистограмма событий НЕВОД-ШАЛ по времени')
    plt.legend()

    plt.savefig(f'./nevod/plots//{date}_anomal_hist_events_by_time.png')
    plt.show()


def save_anomalies_to_json(nanosecond_intervals, date):
    """
    Запись аномальных интервалов в файл формата JSON.
    """
    anomalies_json = [{"anomaly_start": interval[0], "anomaly_end": interval[1]}
                      for interval in nanosecond_intervals]

    with open(f'./nevod/json_files/{date}_anomalies.json', 'w') as f:
        json.dump(anomalies_json, f, indent=4)


# Подключаемся к MongoDB
db_connection = DatabaseConnection()
db_connection.add_database('eas', neas_db)

db_eas = db_connection.get_database('eas')
date = '2018-12-19'
data_e = f'{date}_e'

collection = db_eas[data_e]

data = collection.find({}, {"time_ns": 1})

df = pd.DataFrame(list(data))

intervals, nanosecond_intervals = detect_anomalies(df)

# Сохранение аномалий в файл
save_anomalies_to_json(nanosecond_intervals, date)

bins = np.arange(np.floor(min(df['time_minutes'])), np.ceil(
    max(df['time_minutes'])) + 1)

plot_histogram(df, intervals, bins, date)
