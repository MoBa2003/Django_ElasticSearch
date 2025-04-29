from elasticsearch import Elasticsearch


es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])


index_name = 'electricity_consumption'

mapping = {
    "mappings": {
        "properties": {
            "datetime": {
                "type": "date"
            },
            "global_active_power": {
                "type": "float"
            },
            "global_reactive_power": {
                "type": "float"
            },
            "voltage": {
                "type": "float"
            },
            "global_intensity": {
                "type": "float"
            },
            "sub_metering_1": {
                "type": "float"
            },
            "sub_metering_2": {
                "type": "float"
            },
            "sub_metering_3": {
                "type": "float"
            }
        }
    }
}


def create_index():
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)

create_index()  


import csv
from datetime import datetime
from elasticsearch.helpers import bulk


def ingest_data(csv_file_path):
    actions = []
    
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            action = {
                "_op_type": "index",
                "_index": index_name,
                "_source": {
                    "datetime": datetime.strptime(row['datetime'], '%Y-%m-%dT%H:%M:%S'),
                    "global_active_power": float(row['global_active_power']) if row['global_active_power'] not in ('?', '') else None,
                    "global_reactive_power": float(row['global_reactive_power']) if row['global_reactive_power'] not in ('?', '') else None,
                    "voltage": float(row['voltage']) if row['voltage'] not in ('?', '') else None,
                    "global_intensity": float(row['global_intensity']) if row['global_intensity'] not in ('?', '') else None,
                    "sub_metering_1": float(row['sub_metering_1']) if row['sub_metering_1'] not in ('?', '') else None,
                    "sub_metering_2": float(row['sub_metering_2']) if row['sub_metering_2'] not in ('?', '') else None,
                    "sub_metering_3": float(row['sub_metering_3']) if row['sub_metering_3'] not in ('?', '') else None
                }
            }
            actions.append(action)
    
   
    bulk(es, actions)




ingest_data('D:\\Research\\prj1\\energy_porject\\shortened_dataset.csv')


