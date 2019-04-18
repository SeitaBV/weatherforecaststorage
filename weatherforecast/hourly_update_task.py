import os
import pandas as pd
from weatherforecast.utils import path_to_data, location_utility, get_config
from weatherforecast.utils.Sensor import Sensor
from weatherforecast.utils.weather_forecast_utility import create_forecast_table
import logging


def forecast_is_new(historical_forecast: pd.DataFrame, event_start: str, source: int, sensor_id: str,
                    event_value: float) -> bool:
    """Find out if this specific forecast haws already been added.
    TODO: compare location, as well."""
    if historical_forecast.empty:
        return True
    query = 'event_start == @event_start & source == @source &' \
            ' sensor_id == @sensor_id & event_value == @event_value'
    temp = historical_forecast.query(query)
    return temp.shape[0] == 0


def update_forecast(historical_forecast: pd.DataFrame, current_forecast: pd.DataFrame) -> pd.DataFrame:
    new_entries_list = []
    for forecast in current_forecast.itertuples():
        event_start: str = forecast[1]
        belief_time: str = forecast[2]
        source: int = forecast[3]
        sensor_id: str = forecast[4]
        event_value: float = forecast[5]

        if forecast_is_new(historical_forecast, event_start, source, sensor_id, event_value):
            logging.debug('Adding new entry for %s' % event_start)
            new_entries_list.append({
                'event_start': event_start,
                'belief_time': belief_time,
                'source': source,
                'sensor_id': sensor_id,
                'event_value': event_value
            })

    return pd.DataFrame(new_entries_list, columns=current_forecast.columns)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sensors = Sensor.ALL.value

    logging.info("Reading in existing forecasts ...")
    if os.path.exists('%s/forecasts.csv' % path_to_data()):
        historical_forecast = pd.read_csv('%s/forecasts.csv' % path_to_data())
    else:
        historical_forecast = pd.DataFrame()

    for forecast_location in get_config("LOCATIONS"):
        city, country = [s.strip() for s in forecast_location.split(",")]
     
        logging.info("Getting weather forecasts for %s (%s) ..." % (city, country))

        city_locations = location_utility.get_city_location(city, country)

        logging.info("Asking DarkSky for forecasts and finding out which ones are novel ...")
        current_forecast = create_forecast_table(city_locations, sensors)

        new_entries = update_forecast(historical_forecast, current_forecast)

        logging.info('Adding {} new entries'.format(new_entries.shape[0]))

        if new_entries.shape[0] > 0:
            historical_forecast = historical_forecast.append(new_entries, ignore_index=True, sort=True)
            historical_forecast.to_csv('%s/forecasts.csv' % path_to_data(), index=False, columns=current_forecast.columns)


