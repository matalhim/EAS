from db_connection import DatabaseConnection
from processing import compute_angles_between_vectors, get_theta_values
import matplotlib.pyplot as plt
import numpy as np
import os


db_connection = DatabaseConnection()
db = db_connection.get_db()

plots_folder_name = 'plots'
plots_folder_path = os.path.join(os.getcwd(), plots_folder_name)
if not os.path.exists(plots_folder_path):
    os.makedirs(plots_folder_path)

# Распределение пространственного угла для events
if db is not None:
    average_angles, median_angles = compute_angles_between_vectors(db, events_collection_name='events')

    average_angles = [round(angle) for angle in average_angles]
    median_angles = [round(angle) for angle in median_angles]

    if not average_angles or not median_angles:
        print("Списки average_angles или median_angles пусты после округления.")
    else:
        avg_mean = np.mean(average_angles)
        avg_quantiles = np.percentile(average_angles, [25, 75])

        med_mean = np.mean(median_angles)
        med_quantiles = np.percentile(median_angles, [25, 75])

        min_avg = min(average_angles)
        max_avg = max(average_angles)
        bins_avg = range(int(min_avg), int(max_avg) + 2, 1)

        min_med = min(median_angles)
        max_med = max(median_angles)
        bins_med = range(int(min_med), int(max_med) + 2, 1)

        plt.figure(figsize=(14, 6))

        plt.subplot(1, 2, 1)
        plt.hist(average_angles, bins=bins_avg, color='blue', alpha=0.7, edgecolor='black', align='left')
        plt.xlabel('Угол между векторами')
        plt.ylabel('Число событий')
        plt.title('Распределение углов (средние значения)')

        plt.axvline(avg_mean, color='red', linestyle='dashed', linewidth=2, label=f'Среднее: {avg_mean:.2f}°')
        plt.axvline(avg_quantiles[0], color='orange', linestyle='dashed', linewidth=2,
                    label=f'Q1: {avg_quantiles[0]:.2f}°')
        plt.axvline(avg_quantiles[1], color='orange', linestyle='dashed', linewidth=2,
                    label=f'Q3: {avg_quantiles[1]:.2f}°')

        plt.legend()

        plt.subplot(1, 2, 2)
        plt.hist(median_angles, bins=bins_med, color='blue', alpha=0.7, edgecolor='black', align='left')
        plt.xlabel('Угол между векторами')
        plt.ylabel('Число событий')
        plt.title('Распределение углов (медианные значения)')

        plt.axvline(med_mean, color='red', linestyle='dashed', linewidth=2, label=f'Среднее: {med_mean:.2f}°')
        plt.axvline(med_quantiles[0], color='orange', linestyle='dashed', linewidth=2,
                    label=f'Q1: {med_quantiles[0]:.2f}°')
        plt.axvline(med_quantiles[1], color='orange', linestyle='dashed', linewidth=2,
                    label=f'Q3: {med_quantiles[1]:.2f}°')

        plt.legend()
        plt.tight_layout()

        angles_plot_path = os.path.join(plots_folder_path, 'angles_between_vectors.pdf')
        plt.savefig(angles_plot_path)
        plt.show()
else:
    print("Подключение к базе данных не установлено.")

if db is not None:
    theta_values = get_theta_values(db, collection_name='not_events')
    theta_values = [round(theta) for theta in theta_values]

    if not theta_values:
        print("Нет данных для построения распределения Theta.")
    else:

        min_theta = min(theta_values)
        max_theta = max(theta_values)
        bins_theta = range(int(min_theta), int(max_theta) + 2, 1)  # +2 для включения последнего бина

        plt.figure(figsize=(10, 6))
        plt.hist(theta_values, bins=bins_theta, color='blue', alpha=0.7, edgecolor='black', align='left')
        plt.xlabel('Theta, градусы')
        plt.ylabel('Число событий')
        plt.title('Распределение Theta (not_events)')

        plt.legend()
        plt.tight_layout()


        theta_plot_path = os.path.join(plots_folder_path, 'theta_not_events.pdf')
        plt.savefig(theta_plot_path)
        plt.show()
else:
    print("Подключение к базе данных не установлено.")
