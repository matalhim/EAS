import math
import time
from datetime import datetime
from statistics import mean, median

import matplotlib.pyplot as plt
import numpy as np
from config import BATCH, check_time, dates_list, delta_time, runs_colllect
from pymongo import UpdateOne, errors
from tqdm import tqdm


def plot_groups_per_hour(db, collection_name):
    collection = db[collection_name]
    nabor_numbers = []
    groups_per_hour_lt_55_values = []
    groups_per_hour_gt_55_values = []

    for document in collection.find():
        if "Nabor" in document and "groups_per_hour_gt_55" in document and "groups_per_hour_lt_55" in document:
            nabor = document["Nabor"]
            nabor_number = int(nabor.split("_")[1])
            groups_per_hour_gt_55 = document["groups_per_hour_gt_55"]
            groups_per_hour_lt_55 = document["groups_per_hour_lt_55"]

            nabor_numbers.append(nabor_number)
            groups_per_hour_gt_55_values.append(groups_per_hour_gt_55)
            groups_per_hour_lt_55_values.append(groups_per_hour_lt_55)

    groups_per_hour_values = [
        gt + lt for gt, lt in zip(groups_per_hour_gt_55_values, groups_per_hour_lt_55_values)]
    mean_value = np.mean(groups_per_hour_values)
    median_value = np.median(groups_per_hour_values)

    mean_gt_55 = np.mean(groups_per_hour_gt_55_values)
    mean_lt_55 = np.mean(groups_per_hour_lt_55_values)

    median_gt_55 = np.median(groups_per_hour_gt_55_values)
    median_lt_55 = np.median(groups_per_hour_lt_55_values)

    plt.figure(figsize=(10, 6))
    bar_width = 0.5
    indices = np.arange(len(nabor_numbers))

    plt.bar(indices,
            groups_per_hour_lt_55_values,
            bar_width,
            color='b',
            alpha=0.5,
            label=(r'$\theta \leq 55^{\circ} \qquad \mu_{\theta \leq 55^\circ}= %.3f$' % mean_lt_55) +
            (r'$\qquad M_{\theta \leq 55^\circ}= %.3f$' % median_lt_55)
            )

    plt.bar(indices,
            groups_per_hour_gt_55_values,
            bar_width,
            bottom=groups_per_hour_lt_55_values,
            color='r',
            alpha=0.5,
            label=(r'$\theta > 55^{\circ} \qquad \mu_{\theta > 55^\circ}= %.3f$' % mean_gt_55) +
            (r'$\qquad M_{\theta > 55^\circ}= %.3f$' % median_gt_55)
            )

    plt.xlabel('номер рана')
    plt.ylabel('групп в час')
    plt.ylim(0, 10)
    plt.title('Гистограмма частоты групп мюонов по номеру рана')

    plt.axhline(mean_value, color='g', linestyle='dashed',
                linewidth=1, label=r'$\mu =  %.3f$' % mean_value)
    plt.axhline(median_value, color='orange', linestyle='dashed',
                linewidth=1, label=r'$M =  %.3f$' % median_value)

    plt.xticks(indices, nabor_numbers, rotation=90)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('./nevod/plots/hist_groups_per_hour.png')
    plt.show()


def plot_event_histogram(db, collection_name, date):
    collection = db[collection_name]

    time_ns_values = [doc['time_ns']
                      for doc in collection.find({}, {'time_ns': 1})]

    time_in_minutes = [time / 1e9 / 60 for time in time_ns_values]

    # Построение гистограммы, где каждый столбец соответствует одной минуте
    plt.figure(figsize=(10, 6))
    plt.hist(time_in_minutes, bins=np.arange(np.floor(
        min(time_in_minutes)), np.ceil(max(time_in_minutes)) + 1), edgecolor='black')
    plt.xlabel('Время события (минуты)')
    plt.ylabel('Количество событий')
    plt.title(f'{date}: гистограмма событий по времени')
    plt.grid(axis='y', linestyle='--', linewidth=0.7)
    plt.savefig(f'./nevod/plots/{date}_hist_events+_by_time.png')
    plt.show()


def plot_theta_distribution_all(db_result, not_events_collection_name):
    """
    Функция для построения диаграммы распределения значений Theta для всех документов,
    у которых nevod_eas_is_work = True. На каждый градус один столбик, значения Theta округляются до целых.
    """
    # Фильтрация документов
    filtered_docs = list(db_result[not_events_collection_name].find({
        "nevod_eas_is_work": True
    }))

    # Проверяем, есть ли отфильтрованные документы
    if not filtered_docs:
        print("Нет документов с nevod_eas_is_work = True")
        return

    # Извлечение значений Theta и округление до целых
    theta_values = [round(doc["Theta"])
                    for doc in filtered_docs if "Theta" in doc]

    theta_values_below_55 = [theta for theta in theta_values if theta < 55]

    theta_values_above_or_equal_55 = [
        theta for theta in theta_values if theta >= 55]

    # Построение диаграммы
    plt.figure(figsize=(12, 6))
    plt.hist(theta_values_below_55, bins=range(0, 91), edgecolor='none',
             align='left', rwidth=0.9, alpha=0.7, label=r'$\sum_{\theta < 55^\circ}$' + f'= {len(theta_values_below_55)}')

    plt.hist(theta_values_above_or_equal_55, bins=range(0, 91), edgecolor='none', align='left', rwidth=0.9,
             alpha=0.7, color='red', label=r'$\sum_{\theta \geq 55^\circ}$' + f' = {len(theta_values_above_or_equal_55)}')

    plt.legend(loc='upper right', fontsize=10)
    plt.xlabel(r'$\theta \degree$')
    plt.ylabel('Количество событий')
    plt.ylim(0, 4)
    plt.xticks(range(0, 91, 5))
    plt.yticks(range(0, 4))
    plt.title(
        r'$NRUN = 813 \quad распределение \quad значений \quad \theta$')
    plt.grid(axis='y', linestyle='--', linewidth=0.7)
    plt.savefig('./nevod/plots/813run_hist_theta.png')
    plt.show()
