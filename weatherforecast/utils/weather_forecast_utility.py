import datetime
import os
from typing import Tuple, List
import json
from forecastiopy import ForecastIO
from weatherforecast.utils.helping_tables_utility import read_sensor_location_id_mapping_table
import pandas as pd
from weatherforecast.utils.Source import Source
from datetime import datetime
import configparser

config = configparser.ConfigParser()
config.read('./config/configuration.ini')
API_KEY = config.get('DARK_SKY', 'API_KEY')

sensor_location_id_mapping_df = read_sensor_location_id_mapping_table()


def call_darksky(api_key: str, location: Tuple[float, float]) -> dict:
    """Make a single call to the Dark Sky API and return the result parsed as dict"""
    return ForecastIO.ForecastIO(
        api_key,
        units=ForecastIO.ForecastIO.UNITS_SI,
        lang=ForecastIO.ForecastIO.LANG_ENGLISH,
        latitude=location[0],
        longitude=location[1],
        extend="hourly",
    ).forecast


def save_forecasts_as_json(
        api_key: str, locations: List[Tuple[float, float]], data_path: str
):
    """Get forecasts, then store each as a JSON file"""

    # UTC timestamp to remember when data was fetched.
    now_str = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    os.mkdir("%s/%s" % (data_path, now_str))
    for location in locations:
        forecasts = call_darksky(api_key, location)
        forecasts_file = "%s/%s/forecast_lat_%s_lng_%s.json" % (
            data_path,
            now_str,
            str(location[0]),
            str(location[1]),
        )
        with open(forecasts_file, "w") as outfile:
            json.dump(forecasts, outfile)


def get_sensor_location_id(sensor: str, location_name: str) -> str:
    query = 'sensor == @sensor  & location_name == @location_name'
    return sensor_location_id_mapping_df.query(query)['id'].values[0]


def get_sensor_location_by_id(sensor_id: str) -> (str, str):
    query = 'id == @sensor_id'
    results = sensor_location_id_mapping_df.query(query)[['sensor', 'location_name']].values
    sensor = results[0, 0]
    location_name = results[0, 1]
    return sensor, location_name


def create_forecast_table(locations: pd.DataFrame, sensors_list: List[str], num_hours_to_save: int = 6) -> pd.DataFrame:
    cols = ['event_start', 'belief_time', 'source', 'sensor_id', 'event_value']
    source = Source.DARK_SKY.value
    forecast_list = []
    for location in locations.itertuples():
        latLong = (location[1], location[2])
        location_name = location[3]

        belief_time = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        hourly_forecast_48_list = call_darksky(API_KEY, latLong)['hourly']['data']

        for i in range(0, num_hours_to_save):
            forecast = hourly_forecast_48_list[i]
            event_start = datetime.fromtimestamp(int(forecast['time'])).strftime('%Y-%m-%dT%H-%M-%S')
            retrieve_and_insert_sensors_forecast(belief_time, event_start, forecast, forecast_list, location_name,
                                                 sensors_list, source)

    forecast_df = pd.DataFrame(data=forecast_list, columns=cols)
    forecast_df.to_csv('../data/forecasts.csv', index=False)
    return forecast_df


def retrieve_and_insert_sensors_forecast(belief_time: datetime, event_start: datetime, forecast: dict,
                                         forecast_list: List[dict], location_name: str, sensors_list: List[str],
                                         source: int):
    for sensor in sensors_list:
        sensor_id = get_sensor_location_id(sensor, location_name)
        event_value = forecast[sensor]
        insert_forecast_entry(belief_time, event_start, event_value, forecast_list, sensor_id, source)


def insert_forecast_entry(belief_time: datetime, event_start: datetime, event_value: float, forecast_list: List[dict],
                          sensor_id: str, source: int):
    forecast_list.append({
        'event_start': event_start,
        'belief_time': belief_time,
        'source': source,
        'sensor_id': sensor_id,
        'event_value': event_value
    })
