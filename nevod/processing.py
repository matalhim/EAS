from tqdm import tqdm
from pymongo import UpdateOne, errors
import math
import time
from datetime import datetime
from statistics import mean, median
from config import (runs_colllect, delta_time, check_time, BATCH, dates_list)


def split_collection_by_nrun(db):
    """
    Разделяет исходную коллекцию на отдельные коллекции в зависимости от значения поля 'NRUN'.

    Параметры:
    - db: объект базы данных MongoDB.
    - runs_colllect (str): имя исходной коллекции с документами.

    Результат:
    Создает отдельные коллекции в формате '<NRUN>_run', куда записываются только документы с данным значением NRUN.
    """
    print(f'Разделение коллекции {
          runs_colllect} на отдельные коллекции по полю NRUN.')
    source_collection = db[runs_colllect]
    unique_nrun_values = source_collection.distinct('NRUN')

    for nrun in unique_nrun_values:
        new_collection_name = f"_{nrun}_run"

        matching_documents = source_collection.find({'NRUN': nrun})

        new_collection = db[new_collection_name]
        new_collection.insert_many(matching_documents)

    print("Создание коллекций завершено.")


def create_my_statistic(db, statistica_collect, my_statistica_collect):
    statistica_collection = db[statistica_collect]
    my_statistica_collection = db[my_statistica_collect]

    for document in statistica_collection.find():
        nabor = document.get("Nabor")
        if nabor and nabor.startswith("NAD_"):
            run_number = nabor.split("_")[1]
            corresponding_collection_name = f"_{run_number}_run"
            if corresponding_collection_name in db.list_collection_names():
                corresponding_collection = db[corresponding_collection_name]

                start_time = datetime.strptime(
                    document["Start"], "%d.%m.%y %H:%M")
                stop_time = datetime.strptime(
                    document["Stop"], "%d.%m.%y %H:%M")
                duration = (stop_time - start_time).total_seconds() / 3600
                number_of_groups = corresponding_collection.count_documents({})
                number_of_groups_theta_gt_55 = corresponding_collection.count_documents({
                                                                                        "Theta": {"$gt": 55}})

                new_document = document.copy()
                new_document["duration"] = duration
                new_document["number_of_groups"] = number_of_groups
                new_document["number_of_groups(theta_gt_55)"] = number_of_groups_theta_gt_55

                my_statistica_collection.insert_one(new_document)

    print("Documents have been successfully copied and updated.")


def calculate_groups_per_hour(db, collection_name):
    collection = db[collection_name]
    total_groups = 0
    total_duration = 0
    for document in collection.find():
        if "number_of_groups" in document and "Life_t,hour" in document and document["Life_t,hour"] != 0:
            number_of_groups = document["number_of_groups"]
            number_of_groups_gt = document.get(
                "number_of_groups(theta_gt_55)", 0)
            number_of_groups_lt = number_of_groups - number_of_groups_gt
            duration = document["duration"]

            groups_per_hour = round(number_of_groups / duration, 3)
            groups_per_hour_gt = round(number_of_groups_gt / duration, 3)
            groups_per_hour_lt = round(number_of_groups_lt / duration, 3)

            total_groups += number_of_groups
            total_duration += duration

            result = collection.update_one({"_id": document["_id"]}, {
                "$set": {
                    "groups_per_hour": groups_per_hour,
                    "groups_per_hour_gt_55": groups_per_hour_gt,
                    "groups_per_hour_lt_55": groups_per_hour_lt
                }
            }
            )
            print(total_groups, total_duration)


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


def group_events(db_eas, db_result, data_eas, result_collection):
    collection_eas = db_eas[data_eas]
    collection_result = db_result[result_collection]

    all_documents = list(collection_eas.find())
    grouped_events = []
    visited = set()

    for i, doc in tqdm(enumerate(all_documents), total=len(all_documents), desc="Группировка документов"):
        if i in visited:
            continue

        current_time_ns = doc['time_ns']
        event_group = [doc]
        visited.add(i)

        for j in range(i + 1, len(all_documents)):
            if j in visited:
                continue

            other_doc = all_documents[j]
            other_time_ns = other_doc['time_ns']

            if abs(current_time_ns - other_time_ns) <= delta_time:
                event_group.append(other_doc)
                visited.add(j)

        grouped_events.append(event_group)

    for event_group in tqdm(grouped_events, desc="Запись сгруппированных событий"):
        collection_result.insert_one({'event_documents': event_group})

    print(f"Всего сгруппировано событий: {len(grouped_events)}")


def process_coincidences(db_eas, db_decor, db_result, data_decor, data_events, DATE, time_window_collect):
    """
    Функция для обработки совпадений между коллекциями data_decor и data_events из разных баз данных и сохранения результатов в новую коллекцию.

    Параметры:
    - db_eas: объект базы данных MongoDB для коллекции событий.
    - db_decor: объект базы данных MongoDB для коллекции декора.
    - db_result: объект базы данных MongoDB для сохранения результатов.
    """
    total_events = 0
    print(f'Отбор событий во временном окне {delta_time} нс:')

    db_decor[data_decor].create_index('event_time_ns')
    db_eas[data_events].create_index('eas_event_time_ns')

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
    total_count_result = list(db_decor[data_decor].aggregate(pipeline))
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
    unique_event_time_ns_values = [doc['_id']
                                   for doc in db_decor[data_decor].aggregate(pipeline)]

    batches = [unique_event_time_ns_values[i:i + BATCH]
               for i in range(0, len(unique_event_time_ns_values), BATCH)]

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
                }
            ]

            decor_cursor = db_decor[data_decor].aggregate(
                pipeline, allowDiskUse=True)

            for decor_doc in decor_cursor:
                start_range = decor_doc['start_range']
                end_range = decor_doc['end_range']
                event_time_ns = decor_doc['event_time_ns']

                event_matches = db_eas[data_events].find({
                    'eas_event_time_ns': {'$gte': start_range, '$lte': end_range}
                })

                for event_doc in event_matches:
                    eas_event_time_ns = event_doc['eas_event_time_ns']
                    delta_time_value = eas_event_time_ns - event_time_ns
                    result_document = {
                        'date': DATE,
                        'run': decor_doc.get('run'),
                        'delta_time': delta_time_value,
                        'data_decor_doc': decor_doc,
                        'data_events_doc': event_doc
                    }
                    db_result[time_window_collect].insert_one(result_document)
                    total_events += 1
                    pbar.set_description(f'Отобрано {total_events} события/й)')

            pbar.update(len(batch))


def count_documents_with_large_delta_time(db_result, time_window_collect):
    """
    Функция для подсчета количества документов в коллекции совпадений, у которых abs(delta_time) > check_time.

    Параметры:
    - db_result: объект базы данных MongoDB для сохранения результатов.

    Возвращает:
    - None
    """
    count = db_result[time_window_collect].count_documents({
        '$expr': {
            '$gt': [{'$abs': '$delta_time'}, check_time]
        }
    })
    print(f'Количество документов с abs(delta_time) > {check_time}: {count}')


def add_neas_list_to_coincidences(db_eas, db_result, time_window_collect, data_e):
    """
    Функция для обновления документов в коллекции совпадений, добавляя поле 'neas_list' с полными документами из 'data_e'.

    Параметры:
    - db_eas: объект базы данных MongoDB для коллекции событий.
    - db_result: объект базы данных MongoDB для сохранения результатов.
    """
    total_docs = 0
    print(f'Добавление в {time_window_collect} файлы из {data_e}')

    db_eas[data_e].create_index('_id')

    total_documents = db_result[time_window_collect].count_documents({})

    with tqdm(total=total_documents, desc='Обновление документов') as pbar:
        cursor = db_result[time_window_collect].find(
            {}, no_cursor_timeout=True)

        for doc in cursor:
            list_of_ids = doc['data_events_doc'].get('list_of_ids', [])

            if list_of_ids:
                data_e_docs = list(db_eas[data_e].find(
                    {'_id': {'$in': list_of_ids}}))

                data_e_docs_ordered = []
                data_e_docs_dict = {
                    data_e_doc['_id']: data_e_doc for data_e_doc in data_e_docs}

                for id in list_of_ids:
                    data_e_docs_ordered.append(data_e_docs_dict.get(id))

                db_result[time_window_collect].update_one(
                    {'_id': doc['_id']},
                    {'$set': {'data_e_list': data_e_docs_ordered}}
                )
            else:
                db_result[time_window_collect].update_one(
                    {'_id': doc['_id']},
                    {'$set': {'data_e_list': []}}
                )
            total_docs += 1
            pbar.set_description(f'Обновлено: {total_docs} документов')

            pbar.update(1)

        cursor.close()


def split_TW_documents_by_run(db_result, time_window_collect):
    """
    Функция для разбиения документов в коллекции совпадений по различным значениям 'run' и сохранения их в новые коллекции.

    Параметры:
    - db_result: объект базы данных MongoDB для сохранения результатов.

    Возвращает:
    - None
    """
    runs = db_result[time_window_collect].distinct('run')
    print(f'Количество различных run: {len(runs)}')

    for run in runs:
        run_collection_name = f'RUN_{run}_DATE_{time_window_collect}'
        run_docs = list(db_result[time_window_collect].find({'run': run}))
        if run_docs:
            db_result[run_collection_name].insert_many(run_docs)
        print('\n')
        print(f'run = {run}, docs_count = {len(run_docs)}')

    return runs


def create_events_collection(db, time_window_collect, DATE, run):
    """
    Функция для создания коллекции 'events', содержащей документы из 'coincidences_1000',
    где 'nevod_decor_doc.event_number' равен 'NEvent' из '812_run'.
    В каждый документ добавляется соответствующий документ из '812_run' под ключом '812_run_doc'.
    """

    run_collect = f'_{run}_run'
    run_events = f'RUN_{run}_DATE_{DATE}_events'
    run_TW_collect = f'RUN_{run}_{time_window_collect}'
    db[run_collect].create_index('NEvent')
    total_documents = db[run_TW_collect].count_documents({})

    print(f'Создание коллекции "{run_events}" совместных событий')

    with tqdm(total=total_documents, desc=f'Создание коллекции RUN_{run}_DATE_{DATE}_events') as pbar:
        cursor = db[time_window_collect].find({}, no_cursor_timeout=True)

        events = []
        for doc in cursor:
            event_number = doc['data_decor_doc'].get('event_number')
            nrun = doc.get('run')
            if event_number is not None and nrun == run:
                run_doc = db[run_collect].find_one({'NEvent': event_number})
                if run_doc:
                    doc[f'_{run}_run_doc'] = run_doc
                    events.append(doc)
                    if len(events) >= 1000:
                        db[run_events].insert_many(events)
                        events = []
            pbar.update(1)

        if events:
            db[run_events].insert_many(events)

        cursor.close()
    total_documents = db[run_events].count_documents({})
    print(f"Коллекция '{run_events}' создана. Всего {
          total_documents} документа/ов")


def find_events_by_run(db_result, time_window_collect, DATE, runs):
    """
    Функция пройдется по всем runs за день, и отберет для i-го рана события из _{runs[i]}_run.
    """

    for run in runs:
        create_events_collection(db_result, time_window_collect, DATE, run)


def collect_documents_by_run(db_result):
    run_operations = {}

    # Обходим все коллекции, соответствующие шаблону RUN_<run>_DATE_<date>
    for date in dates_list:
        collections = db_result.list_collection_names()
        for collection_name in collections:
            if collection_name.startswith(f'RUN_') and f'DATE_{date}_events' in collection_name:
                print(f'Обработка коллекции: {collection_name}')
                cursor = db_result[collection_name].find()
                for doc in cursor:
                    run = doc.get('run')
                    if run is not None:
                        collection_key = f'RUN_{run}_events'
                        if collection_key not in run_operations:
                            run_operations[collection_key] = []

                        # Создаем новый документ с правильным порядком ключей
                        new_doc = {
                            'date': doc.get('date'),
                            'run': run,
                            'delta_time': doc.get('delta_time'),
                            f'_{run}_run_doc': doc.get(f'_{run}_run_doc'),
                            'data_decor_doc': doc.get('data_decor_doc'),
                            'data_events_doc': doc.get('data_events_doc'),
                            'data_e_list': doc.get('data_e_list')
                        }

                        run_operations[collection_key].append(
                            UpdateOne(
                                # Условие для поиска существующего документа
                                {'_id': doc['_id']},
                                # Обновление данных документа
                                {'$set': new_doc},
                                upsert=True            # Вставить, если не найден
                            )
                        )

    # Выполняем операции вставки/обновления в новые коллекции RUN_<run>_events
    for collection_key, operations in run_operations.items():
        if operations:
            db_result[collection_key].bulk_write(operations)
            print(f'Сохранено {len(operations)
                               } документов в коллекцию {collection_key}')


def find_missing_documents(db):
    collections = [col for col in db.list_collection_names() if col.startswith(
        'RUN_') and col.count('_') == 2 and col.endswith('_events')]

    for events_collection_name in collections:
        # Extract the run number from the collection name
        run = events_collection_name.split('_')[1]
        run_collection_name = f"_{run}_run"
        not_events_collection_name = f"RUN_{run}_not_events"

        # Retrieve all event numbers from the events collection
        event_numbers = set(doc["data_decor_doc"]["event_number"]
                            for doc in db[events_collection_name].find({}, {"data_decor_doc.event_number": 1}))

        # Find documents in the run collection where NEvent is not in the event numbers
        run_documents = db[run_collection_name].find(
            {"NEvent": {"$nin": list(event_numbers)}})

        # Insert these documents into the not_events collection
        run_documents_list = list(run_documents)
        if len(run_documents_list) > 0:
            db[not_events_collection_name].insert_many(run_documents_list)

    print("Documents have been filtered and inserted successfully.")


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

            db[events_collection_name].update_one(
                {'_id': doc['_id']}, {'$set': {'direction': direction}})

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
                    angle_avg = calculate_angle(
                        average_theta, average_phi, Theta, Phi)
                    average_angles.append(angle_avg)

                if None not in (median_theta, median_phi):
                    angle_med = calculate_angle(
                        median_theta, median_phi, Theta, Phi)
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
