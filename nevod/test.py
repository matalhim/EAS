import json as json
import math
import re
import time
from datetime import datetime, timedelta
from statistics import mean, median

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
from graph_processing import plot_event_histogram, plot_theta_distribution_all
from processing import process_coincidences
from pymongo import MongoClient, UpdateOne, errors
from tqdm import tqdm

# from processing import (split_collection_by_nrun, process_coincidences,
#                         add_neas_list_to_coincidences, count_documents_with_large_delta_time,
#                         split_TW_documents_by_run, find_events_by_run,collect_documents_by_run,
#                         find_missing_documents,)


def process_coincidences_expos(db_eas, db_decor, db_result, data_decor, data_events, DATE, time_window_collect):
    """
    Функция для обработки совпадений между коллекциями data_decor и data_events из разных баз данных 
    и сохранения результатов в новую коллекцию.

    Параметры:
    - db_eas: объект базы данных MongoDB для коллекции событий.
    - db_decor: объект базы данных MongoDB для коллекции декора.
    - db_result: объект базы данных MongoDB для сохранения результатов.
    """
    total_events = 0
    print(f'Отбор событий во временном окне {delta_time} нс:')

    db_decor[data_decor].create_index('event_time_ns')
    db_eas[data_events].create_index('time_ns')

    # Получение уникальных значений event_time_ns из data_decor
    pipeline = [
        {
            '$group': {
                '_id': '$event_time_ns'
            }
        },
        {
            '$sort': {
                '_id': 1
            }
        }
    ]
    unique_event_time_ns_values = [doc['_id']
                                   for doc in db_decor[data_decor].aggregate(pipeline)]

    # Разбиваем данные на батчи
    BATCH = 1000  # Размер батча, можно настроить
    batches = [unique_event_time_ns_values[i:i + BATCH]
               for i in range(0, len(unique_event_time_ns_values), BATCH)]

    with tqdm(total=len(unique_event_time_ns_values), desc='Обработка пакетов') as pbar:
        for batch in batches:
            # Выбор документов из data_decor по batch
            pipeline = [
                {
                    '$match': {
                        'event_time_ns': {'$in': batch}
                    }
                },
                {
                    '$addFields': {
                        'start_range': {'$subtract': ['$event_time_ns', delta_time]},
                        'end_range': {'$add': ['$event_time_ns', delta_time]}
                    }
                }
            ]

            decor_cursor = db_decor[data_decor].aggregate(
                pipeline, allowDiskUse=True)

            for decor_doc in decor_cursor:
                start_range = decor_doc['start_range']
                end_range = decor_doc['end_range']
                event_time_ns = decor_doc['event_time_ns']

                # Поиск соответствующих событий в data_events по time_ns
                event_matches = db_eas[data_events].find({
                    'time_ns': {'$gte': start_range, '$lte': end_range}
                })

                matched_events = []
                for event_doc in event_matches:
                    # Сохраняем каждое найденное событие в список
                    matched_events.append({
                        'eas_event_time_ns': event_doc['time_ns'],
                        'delta_time': event_doc['time_ns'] - event_time_ns,
                        'cluster': event_doc.get('cluster'),
                        'interval': event_doc.get('interval'),
                        'stations': event_doc.get('stations'),
                        'file_name': event_doc.get('file_name'),
                        'position': event_doc.get('position'),
                        'quality': event_doc.get('quality'),
                    })

                # Если есть совпадения, сохраняем результат
                if matched_events:
                    result_document = {
                        'date': DATE,
                        'run': decor_doc.get('run'),
                        'event_time_ns': event_time_ns,
                        'data_decor_doc': decor_doc,
                        'matched_events': matched_events
                    }
                    db_result[time_window_collect].insert_one(result_document)
                    total_events += 1
                    pbar.set_description(f'Отобрано {total_events} событий')

            pbar.update(len(batch))


def find_and_update_coincidences(db_result, db_decor, run_collection, decor_collection, DATE):
    """
    Функция для поиска совпадений между коллекциями RUN_813_not_events и data_decor
    и обновления коллекции RUN_813_not_events добавлением найденных документов из data_decor.

    Параметры:
    - db_result: объект базы данных MongoDB для коллекции RUN_813_not_events.
    - db_decor: объект базы данных MongoDB для коллекции data_decor.
    - run_collection: название коллекции RUN_813_not_events.
    - decor_collection: название коллекции data_decor.
    """
    # Получаем общее количество документов в коллекции для корректной работы tqdm
    total_documents = db_result[run_collection].count_documents({})

    # Переменные для подсчета найденных и ненайденных документов
    found_count = 0
    not_found_count = 0

    # Проходим по каждому документу из коллекции RUN_813_not_events
    run_cursor = db_result[run_collection].find()

    # Создаем прогресс-бар с помощью tqdm
    with tqdm(total=total_documents, desc="Обработка документов") as pbar:
        for run_doc in run_cursor:
            nrun = run_doc.get('NRUN')
            nevent = run_doc.get('NEvent')

            # Ищем документ в data_decor, где run и event_number совпадают с NRUN и NEvent
            decor_doc = db_decor[decor_collection].find_one(
                {'run': nrun, 'event_number': nevent})

            if decor_doc:
                # Извлекаем event_time_ns из документа data_decor
                event_time_ns = decor_doc.get('event_time_ns')

                # Если найден документ, обновляем документ в RUN_813_not_events, добавляя поле data_decor_doc и event_time_ns
                db_result[run_collection].update_one(
                    {'_id': run_doc['_id']},  # Условие для поиска документа
                    {'$set': {
                        'event_time_ns': event_time_ns,  # Сохраняем отдельно event_time_ns
                        'date': DATE,  # Сохраняем дату
                        'data_decor_doc': decor_doc,  # Сохраняем весь документ из data_decor

                    }}
                )
                found_count += 1
            else:
                not_found_count += 1

            # Обновляем прогресс-бар и описание
            pbar.update(1)
            pbar.set_description(f'Найдено: {
                                 found_count}/{total_documents}, Не найдено: {not_found_count}/{total_documents}')


def find_closest_events(db_eas, db_result, not_events_collection_name, data_events_collection_name):
    """
    Функция для нахождения двух документов в коллекции data_events, максимально близких по времени к значению event_time_ns
    в коллекции not_events_collection, и добавления этих документов как left_event_doc и right_event_doc в not_events_collection.

    Параметры:
    - db_eas: объект базы данных MongoDB для коллекции событий (db_eas).
    - db_result: объект базы данных MongoDB для коллекции результатов (db_result).
    - not_events_collection_name: название коллекции not_events_collection в базе данных db_result.
    - data_events_collection_name: название коллекции data_events в базе данных db_eas.
    """

    # Получаем все документы из not_events_collection и считаем их количество для tqdm
    not_events_docs = list(db_result[not_events_collection_name].find())
    total_docs = len(not_events_docs)

    # Создаем прогресс-бар
    with tqdm(total=total_docs, desc="Обработка документов") as pbar:
        for not_event_doc in not_events_docs:
            event_time_ns = not_event_doc.get(
                "data_decor_doc", {}).get("event_time_ns")

            if event_time_ns is None:
                print(f"Документ без event_time_ns, пропускаем: {
                      not_event_doc['_id']}")
                pbar.update(1)
                continue

            # Поиск ближайшего слева (left_event_doc) и справа (right_event_doc)

            # Документ слева: выбираем максимальный eas_event_time_ns, который меньше или равен event_time_ns
            left_event_doc = db_eas[data_events_collection_name].find_one(
                {"eas_event_time_ns": {"$lte": event_time_ns}},
                # Сортировка по убыванию, чтобы найти ближайший слева
                sort=[("eas_event_time_ns", -1)]
            )

            # Документ справа: выбираем минимальный eas_event_time_ns, который больше или равен event_time_ns
            right_event_doc = db_eas[data_events_collection_name].find_one(
                {"eas_event_time_ns": {"$gte": event_time_ns}},
                # Сортировка по возрастанию, чтобы найти ближайший справа
                sort=[("eas_event_time_ns", 1)]
            )

            # Обновляем документ в not_events_collection, добавляя left_event_doc и right_event_doc
            update_fields = {}
            if left_event_doc:
                left_delta_time = left_event_doc["eas_event_time_ns"] - \
                    event_time_ns
                update_fields["left_delta_time"] = left_delta_time
                update_fields["left_event_doc"] = left_event_doc
            if right_event_doc:
                right_delta_time = right_event_doc["eas_event_time_ns"] - \
                    event_time_ns
                update_fields["right_delta_time"] = right_delta_time
                update_fields["right_event_doc"] = right_event_doc

            if update_fields:
                db_result[not_events_collection_name].update_one(
                    {"_id": not_event_doc["_id"]},
                    {"$set": update_fields}
                )
                pbar.set_description(f"Обновлен документ {
                                     not_event_doc["_id"]}")
            else:
                pbar.set_description(f"Не найдены события для документа {
                                     not_event_doc["_id"]}")

            # Обновляем прогресс-бар
            pbar.update(1)

    print("Обработка завершена.")


def find_closest_events_in_data_e(db_eas, db_result, not_events_collection_name, data_e_collection_name):
    """
    Функция для нахождения двух документов в коллекции data_e, максимально близких по времени к значению event_time_ns
    в коллекции not_events_collection, и добавления этих документов как left_e_doc и right_e_doc в not_events_collection.
    Также добавляются left_e_delta_time и right_e_delta_time.
    """

    # Получаем все документы из not_events_collection и считаем их количество для tqdm
    not_events_docs = list(db_result[not_events_collection_name].find())
    total_docs = len(not_events_docs)

    # Создаем прогресс-бар
    with tqdm(total=total_docs, desc="Обработка документов") as pbar:
        for not_event_doc in not_events_docs:
            event_time_ns = not_event_doc.get(
                "data_decor_doc", {}).get("event_time_ns")

            if event_time_ns is None:
                print(f"Документ без event_time_ns, пропускаем: {
                      not_event_doc['_id']}")
                pbar.update(1)
                continue

            # Поиск ближайших событий слева и справа от event_time_ns
            left_event = db_eas[data_e_collection_name].find(
                {'time_ns': {'$lt': event_time_ns}}).sort('time_ns', -1).limit(1)

            right_event = db_eas[data_e_collection_name].find(
                {'time_ns': {'$gt': event_time_ns}}).sort('time_ns', 1).limit(1)

            # Преобразование курсоров в списки
            left_event = list(left_event)
            right_event = list(right_event)

            # Обновление документа в not_events_collection
            update_fields = {}
            if left_event:
                left_e_delta_time = event_time_ns - left_event[0]["time_ns"]
                update_fields["left_e_doc"] = left_event[0]
                update_fields["left_e_delta_time"] = left_e_delta_time

            if right_event:
                right_e_delta_time = right_event[0]["time_ns"] - event_time_ns
                update_fields["right_e_doc"] = right_event[0]
                update_fields["right_e_delta_time"] = right_e_delta_time

            if update_fields:
                db_result[not_events_collection_name].update_one(
                    {"_id": not_event_doc["_id"]},
                    {"$set": update_fields}
                )
                pbar.set_description(f"Обновлен документ {
                                     not_event_doc['_id']}")
            else:
                pbar.set_description(f"Не найдены события для документа {
                                     not_event_doc['_id']}")

            # Обновляем прогресс-бар
            pbar.update(1)

    print("Обработка завершена.")


def group_events(db_eas, db_result, data_evets, result_collection):

    # Получаем документы из data_evets с качеством 'good' и сортируем их по time_ns
    collection_eas = db_eas[data_evets]
    collection_result = db_result[result_collection]

    collection_eas.create_index("time_ns")

    pipeline = [
        {
            '$match': {
                'quality': 'good'
            }
        },
        {
            '$group': {
                '_id': '$time_ns'
            }
        },
        {
            '$sort': {
                '_id': 1
            }
        }
    ]
    unique_time_ns_values = [doc['_id']
                             for doc in collection_eas.aggregate(pipeline)]

    grouped_events = []
    visited_documents = set()
    total_events = 0

    with tqdm(total=len(unique_time_ns_values), desc='Обработка событий') as pbar:
        for time_ns in unique_time_ns_values:
            start_range = time_ns - delta_time
            end_range = time_ns + delta_time

            event_matches = collection_eas.find({
                'time_ns': {'$gte': start_range, '$lte': end_range},
                'quality': 'good'
            })

            event_group = []
            for event in event_matches:
                if event['_id'] not in visited_documents:
                    event_group.append(event)
                    visited_documents.add(event['_id'])

            if event_group:
                grouped_events.append(event_group)
                total_events += len(event_group)
                pbar.set_description(
                    f'Обработка событий (Отобрано документов: {total_events})')

            pbar.update(1)

    with tqdm(total=len(grouped_events), desc="Запись сгруппированных событий") as pbar:
        for event_group in grouped_events:
            collection_result.insert_one({'event_documents': event_group})
            pbar.update(1)

    print(f"Всего сгруппировано событий: {total_events}")


def count_documents_by_list_length(db_eas, db_result, data_events, time_window_collect):

    collection_data_events = db_eas[data_events]
    collection_time_window = db_result[time_window_collect]

    pipeline = [
        {
            '$project': {
                'list_length': {'$size': '$list_of_ids'}
            }
        },
        {
            '$group': {
                '_id': '$list_length',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {
                '_id': 1
            }
        }
    ]
    data_events_count = list(collection_data_events.aggregate(pipeline))
    print("Число документов в коллекции data_events по длине list_of_ids:")
    for item in data_events_count:
        print(f"Длина {item['_id']}: {item['count']} документов")

    # Подсчет документов с различной длиной event_documents в коллекции time_window_collect
    pipeline = [
        {
            '$project': {
                'event_documents_length': {'$size': '$event_documents'}
            }
        },
        {
            '$group': {
                '_id': '$event_documents_length',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {
                '_id': 1
            }
        }
    ]
    time_window_count = list(collection_time_window.aggregate(pipeline))
    print("\nЧисло документов в коллекции time_window_collect по длине event_documents:")
    for item in time_window_count:
        print(f"Длина {item['_id']}: {item['count']} документов")


def count_documents_by_quality(db_eas, data_e):

    collection_data_e = db_eas[data_e]

    pipeline = [
        {
            '$group': {
                '_id': '$quality',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {
                '_id': 1
            }
        }
    ]
    quality_count = list(collection_data_e.aggregate(pipeline))
    print("Число документов в коллекции data_e по значению quality:")
    for item in quality_count:
        print(f"Quality {item['_id']}: {item['count']} документов")


def find_gaps_in_data(db, collection_name, threshold_ns=180e9):
    collection = db[collection_name]

    time_ns_values = [doc['time_ns'] for doc in collection.find(
        {}, {'time_ns': 1}).sort('time_ns', 1)]

    gaps = []
    # Поиск промежутков, превышающих пороговое значение
    for i in range(1, len(time_ns_values)):
        diff = time_ns_values[i] - time_ns_values[i - 1]
        if diff > threshold_ns:
            gaps.append(
                {'start_e': time_ns_values[i - 1], 'stop_e': time_ns_values[i]})

    # Добавление последнего промежутка до конца дня
    if time_ns_values:
        end_of_day_ns = 24 * 60 * 60 * 1e9
        gaps.append({'start_e': time_ns_values[-1], 'stop_e': end_of_day_ns})

    for gap in gaps:
        print(gap)
    return gaps


def find_nearest_events(db, collection_name, target_time_ns):

    collection = db[collection_name]

    left_event = collection.find(
        {'time_ns': {'$lt': target_time_ns}}).sort('time_ns', -1).limit(1)
    right_event = collection.find(
        {'time_ns': {'$gt': target_time_ns}}).sort('time_ns', 1).limit(1)

    left_event = list(left_event)
    right_event = list(right_event)

    # Вывод ближайших событий
    if left_event:
        print(f"Ближайшее событие слева: time_ns = {left_event[0]['time_ns']}")
    else:
        print("Нет событий слева от заданного времени.")

    if right_event:
        print(f"Ближайшее событие справа: time_ns = {
              right_event[0]['time_ns']}")
    else:
        print("Нет событий справа от заданного времени.")


def update_not_events_with_work_status(db_result, not_events_collection_name, not_working_times):
    """
    Функция для обновления документов в коллекции not_events_collection, добавляя флаг nevod_eas_is_work и, если применимо, запись о пропуске (gap).
    """
    # Получаем все документы из not_events_collection и считаем их количество для tqdm
    not_events_docs = list(db_result[not_events_collection_name].find())
    total_docs = len(not_events_docs)

    # Создаем прогресс-бар
    with tqdm(total=total_docs, desc="Обработка документов") as pbar:
        for not_event_doc in not_events_docs:
            event_time_ns = not_event_doc.get(
                "data_decor_doc", {}).get("event_time_ns")
            event_date = not_event_doc.get("date")

            if event_time_ns is None or event_date is None:
                print(f"Документ без event_time_ns или date, пропускаем: {
                      not_event_doc['_id']}")
                pbar.update(1)
                continue

            # Проверка, принадлежит ли event_time_ns какому-либо диапазону нерабочего времени
            nevod_eas_is_work = True
            gap_info = None

            for not_working_day in not_working_times:
                if event_date in not_working_day:
                    for gap in not_working_day[event_date]:
                        if gap['start_e'] <= event_time_ns <= gap['stop_e']:
                            nevod_eas_is_work = False
                            gap_info = gap
                            break
                    break

            # Обновление документа в not_events_collection
            update_fields = {"nevod_eas_is_work": nevod_eas_is_work}

            if not nevod_eas_is_work and gap_info:
                update_fields["gap"] = {
                    "start_e": gap_info['start_e'], "stop_e": gap_info['stop_e']}

            db_result[not_events_collection_name].update_one(
                {"_id": not_event_doc["_id"]},
                {"$set": update_fields}
            )

            # Обновляем прогресс-бар
            pbar.update(1)

    print("Обработка завершена.")


def update_anomaly_flags(db_name, collection_name, date):
    """
    Функция для обновления документов в коллекции на основе аномальных интервалов.

    Аргументы:
    - db_name: Название базы данных.
    - collection_name: Название коллекции, в которой обновляются данные.
    - json_file_path: Путь к JSON-файлу с аномальными интервалами.
    """
    # Функция для проверки попадания event_time_ns в один из аномальных интервалов
    def check_if_anomalous(event_time_ns, anomaly_intervals):
        for interval in anomaly_intervals:
            if interval["anomaly_start"] <= event_time_ns <= interval["anomaly_end"]:
                return False, interval["anomaly_start"], interval["anomaly_end"]
        return True, None, None

    collection = db_name[collection_name]
    json_file_path = f'./nevod/json_files/{date}_anomalies.json'

    # Загрузка аномальных интервалов из JSON файла
    with open(json_file_path, 'r') as f:
        anomaly_intervals = json.load(f)

    # Обрабатываем каждый документ в коллекции
    for document in collection.find():
        event_time_ns = document.get("event_time_ns")

        if event_time_ns is not None:
            flag, start, end = check_if_anomalous(
                event_time_ns, anomaly_intervals)

            update_data = {
                "is_no_anomaly": {
                    "flag": flag
                }
            }

            if not flag:
                update_data["is_no_anomaly"]["start"] = start
                update_data["is_no_anomaly"]["end"] = end

            collection.update_one(
                {"_id": document["_id"]},
                {"$set": update_data}
            )

    print("Обработка завершена.")


db_connection = DatabaseConnection()

db_connection.add_database('eas', neas_db)
db_connection.add_database('decor',  decor_db)
db_connection.add_database('result', result_db)

db_eas = db_connection.get_database('eas')
db_decor = db_connection.get_database('decor')
db_result = db_connection.get_database('result')

not_working_times = list()
for date in dates_list:
    print(f'День: {date}')
    data_decor = f'{date}'
    data_events = f'{date}_events'
    data_e = f'{date}_e'
    time_window_collect = f'{date}__e_events_TW_{delta_time}_ns'

    not_events_collection = 'RUN_813_not_events'
    # process_coincidences_expos(db_eas, db_decor, db_result,
    # not_events_collection, data_e, date, time_window_collect)
    # find_and_update_coincidences(db_result, db_decor, not_events_collection, data_decor, date)
    # find_closest_events(db_eas, db_result, not_events_collection, data_events)
    # find_closest_events_in_data_e(
    #    db_eas, db_result, not_events_collection, data_e)

    # group_events(db_eas, db_result, data_e, time_window_collect)

    # count_documents_by_list_length(
    #     db_eas, db_result, data_events, time_window_collect)

    # count_documents_by_quality(db_eas, data_e)

    #plot_event_histogram(db_eas, data_e, date)
    # gaps = find_gaps_in_data(db_eas, data_e, threshold_ns=180e9)
    # not_working_times.append({date: gaps})

    # find_nearest_events(db_eas, data_e, 12934253777139)

    # print(not_working_times)
    # print(not_working_times)
    #     process_coincidences_expos(db_eas, db_decor, db_result, data_decor,  data_e, date, time_window_collect)

    # update_not_events_with_work_status(
    #     db_result, not_events_collection, not_working_times)

    plot_theta_distribution_all(db_result, not_events_collection)

    # update_anomaly_flags(db_result, not_events_collection, date)
    # break
