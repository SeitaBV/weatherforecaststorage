from weatherforecast.utils import location_utility
from weatherforecast.utils.Sensor import Sensor
import pandas as pd
from hashlib import blake2b


def create_sensor_location_id_mapping_table(save_table=True):
    file_path = '../../data/sensor_location_id_mapping.csv'
    cols = ['id', 'sensor', 'location_name', 'latitude', 'longitude']
    mapping_list = []
    locations = location_utility.get_all_cities_locations()
    sensors = Sensor.ALL.value

    for location in locations.itertuples():
        latitude = location[1]
        longitude = location[2]
        location_name = location[3]

        if isinstance(location_name, str):
            for sensor in sensors:
                data = {
                    'id': create_sensor_location_id(sensor, location_name),
                    'sensor': sensor,
                    'location_name': location_name,
                    'latitude': latitude,
                    'longitude': longitude
                }

                mapping_list.append(data)

    df = pd.DataFrame(data=mapping_list, columns=cols)
    if save_table:
        df.to_csv(file_path, index=False)

    return df


def create_sensor_location_id(sensor, location_name):
    hashing_algo = blake2b(digest_size=10)
    mapping_id = sensor + location_name
    hashing_algo.update(mapping_id.encode('utf-8'))
    return hashing_algo.hexdigest()


def read_sensor_location_id_mapping_table():
    file_path = '../data/sensor_location_id_mapping.csv'
    return pd.read_csv(file_path)