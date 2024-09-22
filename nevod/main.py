from db_connection import DatabaseConnection
from processing import (process_coincidences, add_neas_list_to_coincidences,
                        create_run_collection, create_events_collection, create_not_events_collection, calculate_events_direction)

db_connection = DatabaseConnection()
db = db_connection.get_db()


coincidences_collect_name = 'coincidences_1000_1'
delta_time = 1000
print(f'Отбор событий во врменном окне {delta_time} нс:')
process_coincidences(db, delta_time, coincidences_collect_name, batch_size=1000)

print(f'Добавление в {coincidences_collect_name} файлы из neas_db')
add_neas_list_to_coincidences(db, coincidences_collect_name, neas_db_name='neas_db')

source_collection_name = '810-814_runs'
run_collection_name = '812_run'
nrun_value = 812
print(f'Отбор событий run = {nrun_value}')
create_run_collection(db, source_collection_name, run_collection_name, nrun_value)

events_collection_name = 'events'
not_events_collection_name = 'not_events'
print(f'Создание коллекции "{events_collection_name}" совместных событий')
create_events_collection(db, coincidences_collect_name, run_collection_name, events_collection_name)
print(f'Создание коллекции "{not_events_collection_name}" ненайденных событий декор')
create_not_events_collection(db, coincidences_collect_name, run_collection_name, not_events_collection_name)

print(f'Вычисление медианных и средних углов по кластерам НЕВОД-ШАЛ колекции {events_collection_name}')
calculate_events_direction(db, events_collection_name)

