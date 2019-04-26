import os
import pandas as pd
from weatherforecast.utils import path_to_data, location_utility, get_config
from weatherforecast.utils.Sensor import Sensor
from weatherforecast.utils.weather_forecast_utility import create_forecast_table
import logging


def forecast_is_new(historical_forecast: pd.DataFrame, event_start: str, source: int, sensor_id: str,
                    event_value: float) -> bool:
    """Find out if this specific forecast has already been added.
    TODO: Take belief time into account. We only want to compare to latest relevant belief.
    For instance, we believed x, later y and even later x again. That is all valuable knowledge.
    Should be rare, as beliefs usually become more accurate over time, but still.
    """
    if historical_forecast.empty:
        return True
    query = 'event_start == @event_start & source == @source &' \
            ' sensor_id == @sensor_id & event_value == @event_value'
    temp = historical_forecast.query(query)
    return temp.shape[0] == 0


def filter_out_known_forecast(current_forecasts: pd.DataFrame, historical_forecasts: pd.DataFrame) -> pd.DataFrame:
    new_entries_list = []
    for forecast in current_forecasts.itertuples():
        event_start: str = forecast[1]
        belief_time: str = forecast[2]
        source: int = forecast[3]
        sensor_id: str = forecast[4]
        event_value: float = forecast[5]

        persistence_type = get_config("PERSISTENCE", "type")
        if persistence_type == "file":
            is_new = forecast_is_new(historical_forecasts, event_start, source, sensor_id, event_value)
        elif persistence_type == "db":
            print("TODO: implement db lookup")
            is_new = False
        else:
            raise Exception("Unkown persistence type: %s" % persistence_type)

        if is_new:
            logging.debug('Adding new entry for %s' % event_start)
            new_entries_list.append({
                'event_start': event_start,
                'belief_time': belief_time,
                'source': source,
                'sensor_id': sensor_id,
                'event_value': event_value
            })

    return pd.DataFrame(new_entries_list, columns=current_forecasts.columns)


def save_forecasts(new_entries: pd.DataFrame, historical_forecasts: pd.DataFrame) -> pd.DataFrame:
    if new_entries.shape[0] > 0:
        logging.info('Adding {} new entries.'.format(new_entries.shape[0]))
        persistence_type = get_config("PERSISTENCE", "type")
        if persistence_type == "file":
            historical_forecasts = historical_forecasts.append(new_entries, ignore_index=True, sort=True)
            historical_forecasts.to_csv("%s/%s" % (path_to_data(), get_config("PERSISTENCE", "name")), index=False, columns=new_entries.columns)
        elif persistence_type == "db":
            print("TODO: implement db saving")
        else:
            raise Exception("Unkown persistence type: %s" % persistence_type)
    else:
        logging.info('No new entries.')
    return historical_forecasts



if __name__ == '__main__':
    """
    """
    logging.basicConfig(level=logging.INFO)
    sensors = Sensor.ALL.value

    # reading historical forecasts once, which we will need for file-based storage
    historical_forecasts = pd.DataFrame()
    if get_config("PERSISTENCE", "type") == "file":
        logging.info("Reading in existing forecasts ...")
        if os.path.exists('%s/forecasts.csv' % path_to_data()):
            historical_forecasts = pd.read_csv('%s/forecasts.csv' % path_to_data())

    for forecast_location in get_config("LOCATIONS"):
        city, country = [s.strip() for s in forecast_location.split(",")]
        logging.info("Getting weather forecasts for %s (%s) ..." % (city, country))

        logging.info("Asking DarkSky for forecasts and finding out which ones are novel ...")
        city_locations = location_utility.get_city_location(city, country)
        current_forecasts = create_forecast_table(city_locations, sensors, 12)
        new_entries = filter_out_known_forecast(current_forecasts, historical_forecasts)

        # TODO: this overwrites older locations when using file persistence
        historical_forecasts = save_forecasts(new_entries, historical_forecasts)

