import json
import os
import re

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
from graph_processing import hist_events_by_day
from processing import load_2_mongo

db_connection = DatabaseConnection()


db_connection.add_database('run_events', run_events_db)
db = db_connection.get_database('run_events')

# load_2_mongo(db, folder_path)
hist_events_by_day(db)
