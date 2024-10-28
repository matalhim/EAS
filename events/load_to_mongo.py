import json
import os
import re
import statistics

from config import (
    dates_list,
    decor_db,
    delta_time,
    folder_path,
    my_statistica_collect,
    neas_db,
    result_db,
    run_events_db,
    statistica_collect,
)
from db_connection import DatabaseConnection
from graph_processing import (
    hist_events_by_day,
    plot_delta_time_vs_events,
    plot_events_histogram,
)
from processing import load_2_mongo

db_connection = DatabaseConnection()


db_connection.add_database('run_events', run_events_db)
db = db_connection.get_database('run_events')

# load_2_mongo(db, folder_path)
# hist_events_by_day(db)
# plot_delta_time_vs_events(db)
plot_events_histogram(db, my_statistica_collect)
