import pymongo
import csv
import re
from mongo_processing.db_connection import DatabaseConnection


def export_run_events_to_csv(db_name, output_file):

    db_connection = DatabaseConnection()
    db_connection.add_database('run_events', db_name)
    db = db_connection.get_database('run_events')

    collection_pattern = re.compile(r'^RUN_(\d+)_events$')

    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        header = [
            "NRUN", "NEvent", "Theta", "Phi", "event_time_ns", "clusters_bit", "clusters"
        ]
        for i in range(1, 10):
            for j in range(1, 5):
                header.append(f"{i}_{j}_t")
        for i in range(1, 10):
            header.extend([f"{i}_theta", f"{i}_phi", f"{
                          i}_a_x", f"{i}_a_y", f"{i}_a_z"])
        writer.writerow(header)

        for collection_name in db.list_collection_names():
            match = collection_pattern.match(collection_name)
            if match:
                collection = db[collection_name]
                run_number = match.group(1)

                for document in collection.find():
                    run_doc_key = f'_{run_number}_run_doc'
                    run_doc = document.get(run_doc_key, {})

                    nrun = run_doc.get('NRUN', 'None')
                    nevent = run_doc.get('NEvent', 'None')
                    theta = run_doc.get('Theta', 'None')
                    phi = run_doc.get('Phi', 'None')

                    event_time_ns = document.get('data_decor_doc', {}).get(
                        'event_time_ns', {}).get('$numberLong', 'None')

                    clusters_bit = 0
                    clusters = []
                    for data_e in document.get('data_e_list', []):
                        cluster = data_e.get('cluster')
                        if cluster is not None:
                            # Кластер 1 соответствует первому биту
                            clusters_bit |= (1 << (cluster - 1))
                            clusters.append(cluster)
                    clusters_str = ",".join(map(str, clusters))

                    t_std_values = []
                    for i in range(1, 10):
                        cluster_data = None
                        for data_e in document.get('data_e_list', []):
                            if data_e.get('cluster') == i:
                                cluster_data = data_e.get('stations', {})
                                break
                        for j in range(1, 5):
                            station_key = f'ds_{j}'
                            t_std = cluster_data.get(station_key, {}).get(
                                't_std') if cluster_data else None
                            t_std_values.append(
                                t_std if t_std is not None else 'None')

                    direction_values = []
                    for i in range(1, 10):
                        direction = None
                        for data_e in document.get('data_e_list', []):
                            if data_e.get('cluster') == i:
                                direction = data_e.get('direction', {})
                                break
                        direction_values.extend([
                            direction.get(
                                'theta', 'None') if direction else 'None',
                            direction.get(
                                'phi', 'None') if direction else 'None',
                            direction.get(
                                'a_x', 'None') if direction else 'None',
                            direction.get(
                                'a_y', 'None') if direction else 'None',
                            direction.get(
                                'a_z', 'None') if direction else 'None'
                        ])

                    writer.writerow([nrun, nevent, theta, phi, event_time_ns,
                                    clusters_bit, clusters_str] + t_std_values + direction_values)

    print("Данные успешно записаны в", output_file)
