from config import (
    dates_list,
    decor_db,
    delta_time,
    my_statistica_collect,
    neas_db,
    result_db,
    statistica_collect,
)
from db_connection import DatabaseConnection
from graph_processing import plot_groups_per_hour
from processing import (
    add_neas_list_to_coincidences,
    calculate_groups_per_hour,
    collect_documents_by_run,
    count_documents_with_large_delta_time,
    create_my_statistic,
    find_events_by_run,
    find_missing_documents,
    process_coincidences,
    split_collection_by_nrun,
    split_TW_documents_by_run,
)

db_connection = DatabaseConnection()

db_connection.add_database('eas', neas_db)
db_connection.add_database('decor',  decor_db)
db_connection.add_database('result', result_db)

db_eas = db_connection.get_database('eas')
db_decor = db_connection.get_database('decor')
db_result = db_connection.get_database('result')

# split_collection_by_nrun(db_result)
# create_my_statistic(db_result, statistica_collect, my_statistica_collect)
# calculate_groups_per_hour(db_result, my_statistica_collect)
plot_groups_per_hour(db_result, my_statistica_collect)
# for date in dates_list:
#     print(f'День: {date}')
#     data_decor = f'{date}'
#     data_events = f'{date}_events'
#     data_e = f'{date}_e'
#     time_window_collect = f'{date}_TW_{delta_time}_ns'

#     process_coincidences(db_eas, db_decor, db_result,
#                          data_decor, data_events, date, time_window_collect)

#     # #count_documents_with_large_delta_time(db_result, time_window_collect)

#     add_neas_list_to_coincidences(
#         db_eas, db_result, time_window_collect, data_e)

#     date_runs = split_TW_documents_by_run(db_result, time_window_collect)
#     find_events_by_run(db_result, time_window_collect, date, date_runs)

#     print('\n')

# collect_documents_by_run(db_result)

# find_missing_documents(db_result)
