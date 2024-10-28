import math
import time
from datetime import datetime
from statistics import mean, median, stdev

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.cm as cm
import numpy as np
from config import BATCH, check_time, dates_list, delta_time, runs_colllect
from pymongo import UpdateOne, errors
from tqdm import tqdm


# def plot_groups_per_hour(db, collection_name):
#     collection = db[collection_name]
#     nabor_numbers = []
#     groups_per_hour_lt_55_values = []
#     groups_per_hour_gt_55_values = []

#     for document in collection.find():
#         if "Nabor" in document and "groups_per_hour_gt_55" in document and "groups_per_hour_lt_55" in document:
#             nabor = document["Nabor"]
#             nabor_number = int(nabor.split("_")[1])
#             groups_per_hour_gt_55 = document["groups_per_hour_gt_55"]
#             groups_per_hour_lt_55 = document["groups_per_hour_lt_55"]

#             nabor_numbers.append(nabor_number)
#             groups_per_hour_gt_55_values.append(groups_per_hour_gt_55)
#             groups_per_hour_lt_55_values.append(groups_per_hour_lt_55)

#     groups_per_hour_values = [
#         gt + lt for gt, lt in zip(groups_per_hour_gt_55_values, groups_per_hour_lt_55_values)]
#     mean_value = np.mean(groups_per_hour_values)
#     median_value = np.median(groups_per_hour_values)
#     stdev_value = np.std(groups_per_hour_values)

#     mean_gt_55 = np.mean(groups_per_hour_gt_55_values)
#     mean_lt_55 = np.mean(groups_per_hour_lt_55_values)

#     median_gt_55 = np.median(groups_per_hour_gt_55_values)
#     median_lt_55 = np.median(groups_per_hour_lt_55_values)

#     plt.figure(figsize=(10, 6))
#     bar_width = 0.5
#     indices = np.arange(len(nabor_numbers))

#     # plt.bar(indices,
#     #         groups_per_hour_lt_55_values,
#     #         bar_width,
#     #         edgecolor='black',
#     #         linewidth=4,
#     #         color='b',
#     #         alpha=0.5,
#     #         label=(r'$\theta \leq 55^{\circ} \qquad \mu_{\theta \leq 55^\circ}= %.1f$' % mean_lt_55) +
#     #         (r'$\qquad M_{\theta \leq 55^\circ}= %.1f$' % median_lt_55)
#     #         )

#     # plt.bar(indices,
#     #         groups_per_hour_gt_55_values,
#     #         bar_width,
#     #         bottom=groups_per_hour_lt_55_values,
#     #         edgecolor='black',
#     #         linewidth=4,
#     #         color='r',
#     #         alpha=0.5,
#     #         label=(r'$\theta > 55^{\circ} \qquad \mu_{\theta > 55^\circ}= %.1f$' % mean_gt_55) +
#     #         (r'$\qquad M_{\theta > 55^\circ}= %.1f$' % median_gt_55)
#     #         )

#     plt.bar(indices,
#             groups_per_hour_lt_55_values,
#             bar_width,
#             color='b',
#             alpha=0.5,
#             label=(r'$\theta \leq 55^{\circ} \qquad \mu_{\theta \leq 55^\circ}= %.1f$' % mean_lt_55) +
#             (r'$\qquad M_{\theta \leq 55^\circ}= %.1f$' % median_lt_55)
#             )

#     plt.bar(indices,
#             groups_per_hour_gt_55_values,
#             bar_width,
#             bottom=groups_per_hour_lt_55_values,
#             color='r',
#             alpha=0.5,
#             label=(r'$\theta > 55^{\circ} \qquad \mu_{\theta > 55^\circ}= %.1f$' % mean_gt_55) +
#             (r'$\qquad M_{\theta > 55^\circ}= %.1f$' % median_gt_55)
#             )

#     # 2. Добавляем границы столбцов с полной непрозрачностью
#     plt.bar(indices,
#             groups_per_hour_lt_55_values,
#             bar_width,
#             edgecolor='black',
#             linewidth=2,
#             color='none')  # Используем color='none' для отрисовки только границ

#     plt.bar(indices,
#             groups_per_hour_gt_55_values,
#             bar_width,
#             bottom=groups_per_hour_lt_55_values,
#             edgecolor='black',
#             linewidth=2,
#             color='none')  # Используем color='none' для отрисовки только границ

#     plt.xlabel('номер рана')
#     plt.ylabel('групп в час')
#     plt.ylim(0, 10)
#     plt.title('Гистограмма частоты групп мюонов по номеру рана')

#     plt.axhline(mean_value, color='g', linestyle='dashed',
#                 linewidth=1, label=(r'$\mu =  %.1f \qquad$' % mean_value) + (rf'$\sigma = {stdev_value:.1f}$'))
#     plt.axhline(median_value, color='orange', linestyle='dashed',
#                 linewidth=1, label=r'$M =  %.1f$' % median_value)

#     plt.xticks(indices, nabor_numbers, rotation=90)
#     plt.legend()
#     plt.grid(True)
#     plt.tight_layout()
#     plt.savefig('./nevod/plots/hist_groups_per_hour.png')
#     plt.show()


import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.cm as cm


def plot_groups_per_hour(db, collection_name):
    collection = db[collection_name]
    nabor_numbers = []
    groups_per_hour_lt_55_values = []
    groups_per_hour_gt_55_values = []
    life_t_hours = []

    # Загружаем данные
    for document in collection.find():
        if "Nabor" in document and "groups_per_hour_gt_55" in document and "groups_per_hour_lt_55" in document and "Life_t,hour" in document:
            nabor = document["Nabor"]
            nabor_number = int(nabor.split("_")[1])
            groups_per_hour_gt_55 = document["groups_per_hour_gt_55"]
            groups_per_hour_lt_55 = document["groups_per_hour_lt_55"]
            life_t_hour = document["Life_t,hour"]

            nabor_numbers.append(nabor_number)
            groups_per_hour_gt_55_values.append(groups_per_hour_gt_55)
            groups_per_hour_lt_55_values.append(groups_per_hour_lt_55)
            life_t_hours.append(life_t_hour)

    # Определяем минимальные и максимальные значения для нормализации
    min_life_t_hour = min(life_t_hours)
    max_life_t_hour = max(life_t_hours)

    # Создаем объекты для отображения градиента
    norm = mcolors.Normalize(vmin=min_life_t_hour, vmax=max_life_t_hour)
    cmap_lt_55 = cm.Blues  # Цветовая схема для theta <= 55
    cmap_gt_55 = cm.Reds   # Цветовая схема для theta > 55
    # Берем от 30% до 100% палитры
    cmap_lt_55 = cm.Blues(np.linspace(0.4, 1, 256))
    cmap_lt_55 = mcolors.ListedColormap(cmap_lt_55)

    cmap_gt_55 = cm.Reds(np.linspace(0.4, 1, 256))
    cmap_gt_55 = mcolors.ListedColormap(cmap_gt_55)

    groups_per_hour_values = [
        gt + lt for gt, lt in zip(groups_per_hour_gt_55_values, groups_per_hour_lt_55_values)
    ]
    mean_value = np.mean(groups_per_hour_values)
    median_value = np.median(groups_per_hour_values)
    stdev_value = np.std(groups_per_hour_values)

    mean_gt_55 = np.mean(groups_per_hour_gt_55_values)
    mean_lt_55 = np.mean(groups_per_hour_lt_55_values)

    median_gt_55 = np.median(groups_per_hour_gt_55_values)
    median_lt_55 = np.median(groups_per_hour_lt_55_values)

    fig, ax = plt.subplots(figsize=(14, 8))
    bar_width = 0.5
    indices = np.arange(len(nabor_numbers))

    # Рисуем столбцы с цветом, зависящим от life_t_hours
    for i, index in enumerate(indices):
        # alpha_value = 0.8 + 0.2 * \
        #     (life_t_hours[i] - min_life_t_hour) / \
        #     (max_life_t_hour - min_life_t_hour)
        alpha_value = 1
        # Получаем цвет для каждого столбца из разных цветовых схем
        # Синий для theta <= 55
        color_lt_55 = cmap_lt_55(norm(life_t_hours[i]))
        # Красный для theta > 55
        color_gt_55 = cmap_gt_55(norm(life_t_hours[i]))

        ax.bar(index,
               groups_per_hour_lt_55_values[i],
               bar_width,
               color=color_lt_55,
               alpha=alpha_value,
               label=(r'$\theta \leq 55^{\circ} \qquad \mu_{\theta \leq 55^\circ}= %.1f$' % mean_lt_55) +
                     (r'$\qquad M_{\theta \leq 55^\circ}= %.1f$' % median_lt_55) if i == 0 else "")

        ax.bar(index,
               groups_per_hour_gt_55_values[i],
               bar_width,
               bottom=groups_per_hour_lt_55_values[i],
               color=color_gt_55,
               alpha=alpha_value,
               label=(r'$\theta > 55^{\circ} \qquad \mu_{\theta > 55^\circ}= %.1f$' % mean_gt_55) +
                     (r'$\qquad M_{\theta > 55^\circ}= %.1f$' % median_gt_55) if i == 0 else "")

        # Добавляем границы столбцов с полной непрозрачностью
        ax.bar(index,
               groups_per_hour_lt_55_values[i],
               bar_width,
               edgecolor='black',
               linewidth=2,
               color='none')  # Используем color='none' для отрисовки только границ

        ax.bar(index,
               groups_per_hour_gt_55_values[i],
               bar_width,
               bottom=groups_per_hour_lt_55_values[i],
               edgecolor='black',
               linewidth=2,
               color='none')  # Используем color='none' для отрисовки только границ

    ax.set_xlabel('номер рана', fontsize=14)
    ax.set_ylabel(r'групп в час, $\frac{N}{t}$', fontsize=14)
    ax.set_title(
        'Гистограмма частоты групп мюонов по номеру рана', fontsize=16)
    ax.set_ylim(0, 10)

    custom_line = ax.axhline(y=1, xmin=0.05, xmax=0.15, color='white',
                             linewidth=0.1, label=('2018-12-17 - 2019-02-03\n') + (r'$\sum_{i}N_{групп, i}=6661 \quad \sum_{i}t_{run, i}=1049 $'))

    handles, labels = ax.get_legend_handles_labels()
    handles.insert(0, custom_line)
    labels.insert(0, 'Текст перед элементами')

    ax.axhline(mean_value, color='g', linestyle='dashed',
               linewidth=1, label=(r'$\mu =  %.1f \qquad$' % mean_value) + (rf'$\sigma = {stdev_value:.1f}$'))
    ax.axhline(median_value, color='orange', linestyle='dashed',
               linewidth=1, label=r'$M =  %.1f$' % median_value)

    ax.set_xticks(indices)
    ax.set_xticklabels(nabor_numbers, rotation=90)
    ax.legend()
    ax.grid(True)
    plt.tight_layout()

    # Добавляем цветовую шкалу сбоку для каждого цветового мэппинга
    sm_lt_55 = cm.ScalarMappable(cmap=cmap_lt_55, norm=norm)
    sm_lt_55.set_array([])  # Создаем пустой массив для мэппинга
    cbar_lt = fig.colorbar(sm_lt_55, ax=ax, aspect=30, pad=0.02)
    cbar_lt.set_label(r'$t_{run}$ длительность рана, ч', fontsize=14)

    plt.savefig('./nevod/plots/hist_groups_per_hour.png')
    plt.show()


def plot_event_histogram(db, collection_name, date):
    collection = db[collection_name]

    time_ns_values = [doc['time_ns']
                      for doc in collection.find({}, {'time_ns': 1})]

    time_in_minutes = [time / 1e9 / 60 for time in time_ns_values]

    # Построение гистограммы, где каждый столбец соответствует одной минуте
    plt.figure(figsize=(14, 4))
    plt.hist(time_in_minutes, bins=np.arange(np.floor(
        min(time_in_minutes)), np.ceil(max(time_in_minutes)) + 1), color='black', alpha=0.7)
    plt.xlabel('время регистрации (минуты)')
    plt.ylabel('количество событий')
    plt.title(f'{date}: гистограмма событий НЕВОД-ШАЛ по времени')
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
    plt.hist(theta_values_below_55, bins=range(0, 91), edgecolor='black', linewidth=1,
             align='left', rwidth=0.9, alpha=0.7, label=r'$\sum_{\theta < 55^\circ}$' + f'= {len(theta_values_below_55)}')

    plt.hist(theta_values_above_or_equal_55, bins=range(0, 91), edgecolor='black', linewidth=1, align='left', rwidth=0.9,
             alpha=0.7, color='red', label=r'$\sum_{\theta \geq 55^\circ}$' + f' = {len(theta_values_above_or_equal_55)}')

    plt.legend(loc='upper right', fontsize=12)
    plt.xlabel(r'$\theta \degree$', fontsize=14)
    plt.ylabel('Количество событий', fontsize=14)
    plt.ylim(0, 3.5)
    plt.xticks(range(0, 91, 5))
    plt.yticks(range(0, 4))
    plt.title(
        r'813 ран: распределение значений $\theta$', fontsize=16)
    plt.grid(axis='y', linestyle='--', linewidth=0.7)
    plt.savefig('./nevod/plots/813run_hist_theta.png')
    plt.show()
