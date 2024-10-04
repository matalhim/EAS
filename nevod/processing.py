from tqdm import tqdm
import math
from statistics import mean, median
from config import (data_decor, data_events, data_e, time_window_collect, delta_time, BATCH, RUN,
                    total_runs_collect, run_collect, run_events,merge_collections1, merge_collections2,
                    merge_run_events)


def count_unique_runs(db):
    """
    Функция для подсчета уникальных значений `run` в коллекции и количества документов, им соответствующих.

    Параметры:
        db: объект базы данных MongoDB.
        collection_name (str): имя коллекции, в которой производится поиск.

    Возвращает:
        Словарь, где ключи — уникальные значения `run`, а значения — количество документов.
    """

    global data_decor
    print(f'Подсчет уникальных значений `run` в коллекции {data_decor}:')
    collection = db[data_decor]

    pipeline = [
        {"$group": {"_id": "$run", "count": {"$sum": 1}}}
    ]

    results = collection.aggregate(pipeline)
    for result in results:
        run_value = result["_id"]
        count = result["count"]
        print(f'run = {run_value}: {count} документов')


def process_coincidences(db):
    """
    Функция для обработки совпадений между коллекциями data_decor и 'nevod_eas' и сохранения результатов в новую коллекцию.

    Параметры:
    - db: объект базы данных MongoDB.
    - delta_time (int): временной интервал для поиска совпадений (в наносекундах).
    - BATCH (int): размер пакета для обработки данных.
    - time_window_collect (str): имя коллекции для сохранения результатов.
    """
    global data_decor, data_events, time_window_collect, delta_time, BATCH
    total_events = 0
    print(f'Отбор событий во врменном окне {delta_time} нс:')

    db[data_decor].create_index('event_time_ns')
    db[data_events].create_index('eas_event_time_ns')

    pipeline = [
        {
            '$group': {
                '_id': '$event_time_ns'
            }
        },
        {
            '$count': 'total_count'
        }
    ]
    total_count_result = list(db[data_decor].aggregate(pipeline))
    total_count = total_count_result[0]['total_count'] if total_count_result else 0

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
    unique_event_time_ns_values = [doc['_id'] for doc in db[data_decor].aggregate(pipeline)]

    batches = [unique_event_time_ns_values[i:i + BATCH] for i in range(0, len(unique_event_time_ns_values), BATCH)]

    with tqdm(total=total_count, desc='Обработка пакетов') as pbar:
        for batch in batches:
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
                },
                {
                    '$lookup': {
                        'from': data_events,
                        'let': {
                            'start_range': '$start_range',
                            'end_range': '$end_range'
                        },
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$and': [
                                            {'$gte': ['$eas_event_time_ns', '$$start_range']},
                                            {'$lte': ['$eas_event_time_ns', '$$end_range']}
                                        ]
                                    }
                                }
                            }
                        ],
                        'as': 'event_matches'
                    }
                },
                {
                    '$unwind': '$event_matches'
                },
                {
                    '$unset': ['start_range', 'end_range']
                },
                {
                    '$project': {
                        '_id': 0,
                        'data_decor_doc': '$$ROOT',
                        'data_events_doc': '$event_matches'
                    }
                }
            ]

            cursor = db[data_decor].aggregate(pipeline, allowDiskUse=True)

            results = list(cursor)
            if results:
                db[time_window_collect].insert_many(results)
                total_events += len(results)
                pbar.set_description(f'Отобрано {total_events} события/й)')

            pbar.update(len(batch))

def add_neas_list_to_coincidences(db):
    """
    Функция для обновления документов в коллекции совпадений, добавляя поле 'neas_list' с полными документами из 'data_e'.

    Параметры:
    - db: объект базы данных MongoDB.
    - time_window_collect (str): название коллекции совпадений для обновления.
    - data_e (str): название коллекции 'data_e', содержащей документы для добавления.

    Возвращает:
    - None
    """
    global time_window_collect, data_e
    total_docs = 0
    print(f'Добавление в {time_window_collect} файлы из {data_e}')

    db[data_e].create_index('_id')

    total_documents = db[time_window_collect].count_documents({})

    with tqdm(total=total_documents, desc='Обновление документов') as pbar:
        cursor = db[time_window_collect].find({}, no_cursor_timeout=True)

        for doc in cursor:
            list_of_ids = doc['data_events_doc'].get('list_of_ids', [])

            if list_of_ids:
                data_e_docs = list(db[data_e].find({'_id': {'$in': list_of_ids}}))

                data_e_docs_ordered = []
                data_e_docs_dict = {data_e_doc['_id']: data_e_doc for data_e_doc in data_e_docs}

                for id in list_of_ids:
                    data_e_docs_ordered.append(data_e_docs_dict.get(id))

                db[time_window_collect].update_one(
                    {'_id': doc['_id']},
                    {'$set': {'data_e_list': data_e_docs_ordered}}
                )
            else:
                db[time_window_collect].update_one(
                    {'_id': doc['_id']},
                    {'$set': {'data_e_list': []}}
                )
            total_docs += len(list_of_ids)
            pbar.set_description(f'Обновлено: {total_docs} документов')

            pbar.update(1)

        cursor.close()

def create_run_collection(db):
    """
    Функция для создания новой коллекции, содержащей документы из исходной коллекции с заданным значением поля 'NRUN'.

    Параметры:
    - db: объект базы данных MongoDB.
    - time_window_collect (str): название исходной коллекции.
    - target_collection_name (str): название новой коллекции для сохранения результатов.
    - nrun_value (int): значение поля 'NRUN' для фильтрации документов.

    Возвращает:
    - None
    """
    global RUN, total_runs_collect, run_collect, BATCH
    print(f'Отбор событий run = {RUN}')

    filter_query = {'NRUN': RUN}

    total_documents = db[total_runs_collect].count_documents(filter_query)

    if total_documents > 0:
        with tqdm(total=total_documents, desc=f"Копирование документов с NRUN={RUN}") as pbar:
            BATCH= 1000
            cursor = db[total_runs_collect].find(filter_query).batch_size(BATCH)

            documents_batch = []
            for document in cursor:
                documents_batch.append(document)
                pbar.update(1)

                if len(documents_batch) >= BATCH:
                    db[run_collect].insert_many(documents_batch)
                    documents_batch = []

            if documents_batch:
                db[run_collect].insert_many(documents_batch)

        total_documents = db[run_collect].count_documents({})
        print(f"{total_documents} документа/ов с NRUN={RUN} из коллекции '{total_runs_collect}' скопированы в новую коллекцию '{run_collect}'.")
    else:
        print(f"В коллекции '{total_runs_collect}' не найдено документов с NRUN={RUN}.")


def check_run(db):
    global RUN, time_window_collect
    print(f'Проверка событий run = {RUN} в коллекции {time_window_collect}')
    collection = db[time_window_collect]

    result = collection.delete_many({"data_decor_doc.run": {"$ne": RUN}})
    remaining_docs = collection.count_documents({})

    print(f"Удалено документов: {result.deleted_count}")
    print(f"Осталось документов: {remaining_docs}")


def create_events_collection(db):
    """
    Функция для создания коллекции 'events', содержащей документы из 'coincidences_1000',
    где 'nevod_decor_doc.event_number' равен 'NEvent' из '812_run'.
    В каждый документ добавляется соответствующий документ из '812_run' под ключом '812_run_doc'.
    """
    global time_window_collect, run_collect, run_events
    print(f'Создание коллекции "{run_events}" совместных событий')

    db[run_collect].create_index('NEvent')
    total_documents = db[time_window_collect].count_documents({})

    with tqdm(total=total_documents, desc='Создание коллекции events') as pbar:
        cursor = db[time_window_collect].find({}, no_cursor_timeout=True)

        events = []
        for doc in cursor:
            event_number = doc['data_decor_doc'].get('event_number')
            if event_number is not None:
                run_doc = db[run_collect].find_one({'NEvent': event_number})
                if run_doc:
                    doc['812_run_doc'] = run_doc
                    events.append(doc)
                    if len(events) >= 1000:
                        db[run_events].insert_many(events)
                        events = []
            pbar.update(1)

        if events:
            db[run_events].insert_many(events)

        cursor.close()
    total_documents = db[run_events].count_documents({})
    print(f"Коллекция '{run_events}' создана. Всего {total_documents} документа/ов")


def merge_collections(db):
    """
    Функция для объединения двух коллекций и записи результатов в новую коллекцию.

    Параметры:
        db: объект базы данных MongoDB.
        collection1 (str): имя первой коллекции.
        collection2 (str): имя второй коллекции.
        new_collection (str): имя новой коллекции, в которую будут записаны объединенные документы.
    """
    global merge_collections1, merge_collections2, merge_run_events

    col1 = db[merge_collections1]
    col2 = db[merge_collections2]
    target_collection = db[merge_run_events]

    documents_from_col1 = list(col1.find({}))
    documents_from_col2 = list(col2.find({}))

    combined_documents = documents_from_col1 + documents_from_col2

    if combined_documents:
        target_collection.insert_many(combined_documents)

    total_docs = target_collection.count_documents({})

    print(f"Всего документов в новой коллекции '{merge_run_events}': {total_docs}")

def create_not_events_collection(db, coincidences_collection_name, run_collection_name, not_events_collection_name):
    """
    Функция для создания коллекции 'not_events', содержащей документы из '812_run',
    у которых 'NEvent' не совпадает с 'event_number' ни в одном документе из 'coincidences_1000'.
    """
    global time_window_collect, run_collect, run_not_events
    event_numbers = set()
    cursor = db[time_window_collect].find({}, {'data_decor_doc.event_number': 1})
    for doc in cursor:
        event_number = doc[data_decor].get('event_number')
        if event_number is not None:
            event_numbers.add(event_number)
    cursor.close()

    total_documents = db[run_collect].count_documents({})
    with tqdm(total=total_documents, desc='Создание коллекции not_events') as pbar:
        cursor = db[run_collect].find({}, no_cursor_timeout=True)
        not_events = []
        for doc in cursor:
            n_event = doc.get('NEvent')
            if n_event not in event_numbers:
                not_events.append(doc)
                # Вставляем документы пакетами
                if len(not_events) >= 1000:
                    db[run_not_events].insert_many(not_events)
                    not_events = []
            pbar.update(1)

        if not_events:
            db[run_not_events].insert_many(not_events)

        cursor.close()
    print(f"Коллекция '{run_not_events}' создана.")

def calculate_events_direction(db, events_collection_name):

    total_documents = db[events_collection_name].count_documents({})

    with tqdm(total=total_documents, desc='Обновление событий') as pbar:
        cursor = db[events_collection_name].find({}, no_cursor_timeout=True)

        for doc in cursor:
            theta_values = []
            phi_values = []

            neas_list = doc.get('neas_list', [])
            for neas_doc in neas_list:
                direction = neas_doc.get('direction', {})
                theta = direction.get('theta')
                phi = direction.get('phi')

                if theta is not None:
                    theta_values.append(theta)
                if phi is not None:
                    phi_values.append(phi)

            if theta_values:
                average_theta = mean(theta_values)
                median_theta = median(theta_values)
            else:
                average_theta = None
                median_theta = None

            if phi_values:
                average_phi = mean(phi_values)
                median_phi = median(phi_values)
            else:
                average_phi = None
                median_phi = None

            run_doc = doc.get('812_run_doc', {})
            Theta_812_run_doc = run_doc.get('Theta')
            Phi_812_run_doc = run_doc.get('Phi')

            direction = {
                'average_theta': average_theta,
                'average_phi': average_phi,
                'median_theta': median_theta,
                'median_phi': median_phi,
                'Theta': Theta_812_run_doc,
                'Phi': Phi_812_run_doc
            }

            db[events_collection_name].update_one({'_id': doc['_id']}, {'$set': {'direction': direction}})

            pbar.update(1)

        cursor.close()

def compute_angles_between_vectors(db, events_collection_name):
    """
    Функция для вычисления пространственных углов между векторами, заданными углами из документов коллекции 'events'.

    Параметры:
    - db: объект базы данных MongoDB.
    - events_collection_name (str): название коллекции с событиями.

    Возвращает:
    - average_angles (list): список углов (в градусах) между (average_theta, average_phi) и (Theta, Phi).
    - median_angles (list): список углов (в градусах) между (median_theta, median_phi) и (Theta, Phi).
    """
    average_angles = []
    median_angles = []
    def calculate_angle(theta1, phi1, theta2, phi2):
        """
        Вычисляет угол между двумя векторами, заданными углами theta и phi.

        Параметры:
        - theta1, phi1: углы первого вектора (в градусах).
        - theta2, phi2: углы второго вектора (в градусах).

        Возвращает:
        - angle_degrees: угол между векторами в градусах.
        """
        theta1_rad = math.radians(theta1)
        phi1_rad = math.radians(phi1)
        theta2_rad = math.radians(theta2)
        phi2_rad = math.radians(phi2)

        v1_x = math.sin(theta1_rad) * math.cos(phi1_rad)
        v1_y = math.sin(theta1_rad) * math.sin(phi1_rad)
        v1_z = math.cos(theta1_rad)

        v2_x = math.sin(theta2_rad) * math.cos(phi2_rad)
        v2_y = math.sin(theta2_rad) * math.sin(phi2_rad)
        v2_z = math.cos(theta2_rad)

        dot_product = v1_x * v2_x + v1_y * v2_y + v1_z * v2_z

        dot_product = max(min(dot_product, 1.0), -1.0)

        angle_rad = math.acos(dot_product)
        angle_degrees = math.degrees(angle_rad)

        return angle_degrees

    total_documents = db[events_collection_name].count_documents({})

    with tqdm(total=total_documents, desc='Вычисление углов') as pbar:
        cursor = db[events_collection_name].find({}, no_cursor_timeout=True)

        for doc in cursor:
            direction = doc.get('direction', {})

            average_theta = direction.get('average_theta')
            average_phi = direction.get('average_phi')
            median_theta = direction.get('median_theta')
            median_phi = direction.get('median_phi')
            Theta = direction.get('Theta')
            Phi = direction.get('Phi')

            if None not in (Theta, Phi):
                if None not in (average_theta, average_phi):
                    angle_avg = calculate_angle(average_theta, average_phi, Theta, Phi)
                    average_angles.append(angle_avg)

                if None not in (median_theta, median_phi):
                    angle_med = calculate_angle(median_theta, median_phi, Theta, Phi)
                    median_angles.append(angle_med)

            pbar.update(1)

        cursor.close()
    return average_angles, median_angles

def get_theta_values(db, collection_name):
    """
    Функция для извлечения значений 'Theta' из указанной коллекции.

    Параметры:
    - db: объект базы данных MongoDB.
    - collection_name (str): название коллекции, из которой будут извлекаться значения 'Theta'.

    Возвращает:
    - theta_values (list): список значений 'Theta'.
    """
    theta_values = []
    total_documents = db[collection_name].count_documents({})
    with tqdm(total=total_documents, desc=f'Извлечение Theta из {collection_name}') as pbar:
        cursor = db[collection_name].find({}, {'Theta': 1, '_id': 0})
        for doc in cursor:
            theta = doc.get('Theta')
            if theta is not None:
                try:
                    theta = float(theta)
                    theta_values.append(theta)
                except (TypeError, ValueError):
                    pass
            pbar.update(1)
        cursor.close()
    return theta_values