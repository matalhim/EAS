from db_connection import DatabaseConnection
from config import database_name
from processing import (count_unique_runs, process_coincidences, add_neas_list_to_coincidences,
                        create_run_collection, check_run, create_events_collection,
                        create_not_events_collection, calculate_events_direction, merge_collections)

db_connection = DatabaseConnection(database_name=database_name)
db = db_connection.get_db()

# Подсчет уникальных значений `run` в коллекции data_decor
count_unique_runs(db)
# Поиск событий за один день во врменном окне delta_time
process_coincidences(db)
# добовавление информации о кластерах шевод-шал
add_neas_list_to_coincidences(db)
#создание колекции рана декор
#create_run_collection(db)
# проверка на ран time_window_collect
check_run(db)
# совместные события за этот день и интересующий ран
create_events_collection(db)
# одьеденение различных дней одного рана
#merge_collections(db)
# print(f'Вычисление медианных и средних углов по кластерам НЕВОД-ШАЛ колекции {events_collection_name}')
# calculate_events_direction(db, events_collection_name)