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
import logging

from weatherforecast.utils import path_to_data, path_to_config

config = configparser.ConfigParser()
config.read('%s/configuration.ini' % path_to_config())
API_KEY: str = config.get('DARK_SKY', 'API_KEY')

sensor_location_id_mapping_df = read_sensor_location_id_mapping_table()


def call_darksky(api_key: str, location: Tuple[float, float]) -> dict:
    """Make a single call to the Dark Sky API and return the result parsed as dict"""
    logging.debug("Forecasting for this location {}".format(location))
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
    logging.debug('Getting sensor id for %s %s', sensor, location_name)
    query = 'sensor == @sensor  & location_name == @location_name'
    return sensor_location_id_mapping_df.query(query)['id'].values[0]


def get_sensor_location_by_id(sensor_id: str) -> (str, str):
    query = 'id == @sensor_id'
    results = sensor_location_id_mapping_df.query(query)[['sensor', 'location_name']].values
    sensor = results[0, 0]
    location_name = results[0, 1]
    return sensor, location_name


def create_forecast_table(locations: pd.DataFrame, sensors_list: List[str], num_hours_to_save: int = 6,
                          overwrite_forecast_file: bool = False) -> pd.DataFrame:
    logging.debug("Creating forecast table using these sensors: {} "
                  "and saving the next {} hours".format(sensors_list, num_hours_to_save))

    cols = ['event_start', 'belief_time', 'source', 'sensor_id', 'event_value']
    source = Source.DARK_SKY.value
    forecast_list = []
    for location in locations.itertuples():
        lat_long = (location[1], location[2])
        location_name = location[3]
        logging.debug("Getting forecast for this location: {}".format(location_name))

        belief_time = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        hourly_forecast_48_list = call_darksky(API_KEY, lat_long)['hourly']['data']

        for i in range(0, num_hours_to_save):
            forecast = hourly_forecast_48_list[i]
            event_start = datetime.fromtimestamp(int(forecast['time'])).strftime('%Y-%m-%dT%H-%M-%S')
            retrieve_and_insert_sensors_forecast(belief_time, event_start, forecast, forecast_list, location_name,
                                                 sensors_list, source)

    forecast_df = pd.DataFrame(data=forecast_list, columns=cols)

    if overwrite_forecast_file:
        forecast_df.to_csv('%s/forecasts.csv' % path_to_data(), index=False, columns=cols)

    return forecast_df


def retrieve_and_insert_sensors_forecast(belief_time: str, event_start: str, forecast: dict,
                                         forecast_list: List[dict], location_name: str, sensors_list: List[str],
                                         source: int):
    logging.debug("Retrieving and inserting sensors' values for this location %s", location_name)
    for sensor in sensors_list:
        sensor_id = get_sensor_location_id(sensor, location_name)
        event_value = forecast[sensor]
        insert_forecast_entry(belief_time, event_start, event_value, forecast_list, sensor_id, source)


def insert_forecast_entry(belief_time: str, event_start: str, event_value: float, forecast_list: List[dict],
                          sensor_id: str, source: int):
    logging.debug(
        "Inserting new entry: {}, {}, {}, {}, {}".format(event_start, belief_time, source, sensor_id, event_value))
    forecast_list.append({
        'event_start': event_start,
        'belief_time': belief_time,
        'source': source,
        'sensor_id': sensor_id,
        'event_value': event_value
    })
