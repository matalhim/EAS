import datetime
import math
import time
from collections import Counter, defaultdict
from datetime import datetime
from statistics import mean, median, stdev, variance

import matplotlib.cm as cm
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
from config import BATCH, check_time, dates_list, delta_time, runs_colllect
from pymongo import UpdateOne, errors
from tqdm import tqdm


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


def plot_delta_time_vs_events(db):
    delta_time_list = []

    for collection_name in db.list_collection_names():
        collection = db[collection_name]

        # Проход по всем документам в коллекции
        for document in collection.find():
            if "delta_time" in document:
                delta_time = document["delta_time"]
                if abs(delta_time) < 700:
                    delta_time_list.append(abs(delta_time))

    mean_delta_time = mean(delta_time_list)
    median_delta_time = median(delta_time_list)
    stdev_delta_time = stdev(delta_time_list)

    plt.figure(figsize=(15, 8))

    # Настраиваем bins, чтобы метки совпадали с центром бина (например, 100, 200 и т.д.)
    bin_width = 10
    bins = np.arange(0, 700 + bin_width, bin_width)

    counts, edges, _ = plt.hist(
        delta_time_list,
        bins=bins,
        alpha=0.7,
        edgecolor='black',
        linewidth=1.5,
        color='#D2691E',
        label=(f'2018-12-19 - 2019-02-02\n') +
        (rf'$\sum_i n_i= ${len(delta_time_list)}')
    )

    # Выставляем метки под каждые 100, ровно под соответствующие бины
    tick_positions = np.arange(100, 701, 100)
    plt.xticks(tick_positions, [
               f"{int(pos)}" for pos in tick_positions], fontsize=15)

    plt.yticks(fontsize=15)

    # Добавляем вертикальные линии для среднего, медианы и стандартного отклонения
    plt.axvline(mean_delta_time, color='r', linestyle='dashed',
                linewidth=2, label=fr'$\mu = {mean_delta_time:.1f}$')
    plt.axvline(median_delta_time, color='b', linestyle='dashed',
                linewidth=2, label=fr'$M = {median_delta_time:.1f}$')
    plt.axvline(mean_delta_time + stdev_delta_time, color='black', linestyle='dashed',
                linewidth=2, label=fr'$\sigma = {stdev_delta_time:.1f}$')
    plt.axvline(mean_delta_time - stdev_delta_time, color='black', linestyle='dashed',
                linewidth=2)

    plt.xlim(0, 650)
    plt.xlabel(
        r'временной интервал, $\Delta t = t_{Д} - t_{НШ}$, нс', fontsize=20)
    plt.ylabel("Число событий, n", fontsize=20)

    plt.title("Распределение числа совместных событий по временному интервалу между ДЕКОР и НЕВОД-ШАЛ", fontsize=20)
    plt.grid(axis='y')
    plt.legend(fontsize=15)
    plt.savefig('./events/plots/hist_delta_time_vs_events.png')
    plt.show()


def plot_events_histogram(db, my_statistica_collection_name):
    my_statistica_collection = db[my_statistica_collection_name]

    runs = []
    number_of_groups_list = []
    not_found_events_list = []
    life_t_hours_list = []

    for document in my_statistica_collection.find():
        nabor = document['Nabor']
        run_number = int(nabor.split('_')[1])
        number_of_groups = document['number_of_groups']
        life_t_hour = document['Life_t,hour']

        run_collection_name = f'RUN_{run_number}_events'
        not_found_events_count = db[run_collection_name].count_documents(
            {})

        runs.append(run_number)
        number_of_groups_list.append(number_of_groups)
        not_found_events_list.append(not_found_events_count)
        life_t_hours_list.append(life_t_hour)

    runs = np.array(runs)
    number_of_groups_list = np.array(number_of_groups_list)
    not_found_events_list = np.array(not_found_events_list)
    life_t_hours_list = np.array(life_t_hours_list)

    min_life_t_hour = min(life_t_hours_list)
    max_life_t_hour = max(life_t_hours_list)
    norm = mcolors.Normalize(vmin=min_life_t_hour, vmax=max_life_t_hour)

    cmap_total = cm.Reds(np.linspace(0.4, 1, 256))
    cmap_total = mcolors.ListedColormap(cmap_total)
    cmap_not_found = cm.Greens(np.linspace(0.4, 1, 256))
    cmap_not_found = mcolors.ListedColormap(cmap_not_found)

    fig, ax = plt.subplots(figsize=(14, 8))
    bar_width = 0.6
    indices = np.arange(len(runs))

    for i, index in enumerate(indices):
        color_total = cmap_total(norm(life_t_hours_list[i]))
        color_not_found = cmap_not_found(norm(life_t_hours_list[i]))

        ax.bar(index,
               number_of_groups_list[i],
               bar_width,
               color=color_total,
               edgecolor='black',
               linewidth=2,
               label="ненайденные события" if i == 0 else "")

        ax.bar(index,
               not_found_events_list[i],
               bar_width,
               bottom=number_of_groups_list[i] - not_found_events_list[i],
               color=color_not_found,
               edgecolor='black',
               linewidth=2,
               label="число событий" if i == 0 else "")

    ax.set_xlabel('RUN', fontsize=20)
    ax.set_ylabel('число событий', fontsize=20)
    ax.set_title(
        'Гистограмма числа событий групп мюонов по RUN', fontsize=20)
    ax.set_xticks(indices)
    ax.set_xticklabels(runs, rotation=90, fontsize=15)
    ax.set_yticklabels(ax.get_yticks(), fontsize=15)
    ax.set_ylim(0, 300)

    sm_life_t = cm.ScalarMappable(cmap=cmap_not_found, norm=norm)
    sm_life_t.set_array([])
    cbar_life_t = fig.colorbar(sm_life_t, ax=ax, aspect=30, pad=0.02)
    cbar_life_t.set_label(r'$t_{run}$ длительность RUN, ч', fontsize=20)

    ax.legend(loc='upper right', fontsize=16)
    plt.tight_layout()
    plt.savefig('./events/plots/plot_events_histogram.png')
    plt.show()
