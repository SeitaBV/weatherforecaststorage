from datetime import datetime
import os
import logging
from typing import Tuple, List
import json

import pytz
import pandas as pd
from forecastiopy import ForecastIO
from timely_beliefs import (
    BeliefsDataFrame,
    TimedBelief,
    DBTimedBelief,
    Sensor,
    BeliefSource,
    DBBeliefSource,
)

from weatherforecast.utils.dbconfig import DBLocatedSensor, get_or_create_sensors_for
from weatherforecast.utils.helping_tables_utility import (
    read_sensor_location_id_mapping_table,
)
from weatherforecast.utils import (
    cols,
    get_config,
    path_to_data,
    path_to_config,
    dbconfig,
)


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
    logging.debug("Getting sensor id for %s %s", sensor, location_name)
    query = "sensor == @sensor  & location_name == @location_name"
    return sensor_location_id_mapping_df.query(query)["id"].values[0]


def get_sensor_location_by_id(sensor_id: str) -> (str, str):
    query = "id == @sensor_id"
    results = sensor_location_id_mapping_df.query(query)[
        ["sensor", "location_name"]
    ].values
    sensor = results[0, 0]
    location_name = results[0, 1]
    return sensor, location_name


def create_forecasts(
    locations: pd.DataFrame, sensor_names: List[str], num_hours_to_save: int = 6
) -> List[TimedBelief]:
    logging.debug(
        "Creating forecasts using these sensors: {} "
        "and saving the next {} hours".format(sensor_names, num_hours_to_save)
    )

    if get_config("PERSISTENCE", "type") == "file":
        source = BeliefSource(name="DarkSky")
    else:
        # session = dbconfig.create_db_and_session()
        from weatherforecast.utils.dbconfig import session

        source = session.query(DBBeliefSource).filter_by(name="DarkSky").first()

    api_key = get_config("DARK_SKY", "API_KEY")
    forecast_list = []
    for location in locations.itertuples():
        lat_long = (location[1], location[2])
        location_name = location[3]
        belief_time = datetime.utcnow().replace(tzinfo=pytz.utc)

        logging.debug(
            "Getting forecasts for {} at belief time {} ...".format(
                location_name, belief_time
            )
        )
        hourly_forecast_48_list = call_darksky(api_key, lat_long)["hourly"]["data"]

        if get_config("PERSISTENCE", "type") == "file":
            sensors = [
                Sensor(name=sname) for sname in sensor_names
            ]  # TODO: convey the special ID here (use a class with string ID?)
        else:
            sensors = get_or_create_sensors_for(
                session, lat_long[0], lat_long[1], location_name
            )

        for i in range(0, num_hours_to_save):
            forecast = hourly_forecast_48_list[i]
            event_start = datetime.fromtimestamp(int(forecast["time"]), tz=pytz.utc)
            add_forecast_for_sensors(
                event_start,
                belief_time,
                forecast,
                location_name,
                source,
                forecast_list,
                sensors,
            )

    return forecast_list


def add_forecast_for_sensors(
    event_start: datetime,
    belief_time: datetime,
    forecast: dict,
    location_name: str,
    source: BeliefSource,
    forecast_list: List[TimedBelief],
    sensors: List[Sensor],
):
    """Add new forecast to the list, for each sensor"""
    for sensor in sensors:
        # sensor_id = get_sensor_location_id(sensor.name, location_name)
        event_value = forecast[sensor.name]
        append_forecast_entry(
            event_start, belief_time, event_value, sensor, source, forecast_list
        )


def append_forecast_entry(
    event_start: str,
    belief_time: str,
    event_value: float,
    sensor: Sensor,
    source: BeliefSource,
    forecast_list: List[TimedBelief],
):
    """Decide which TimedBelief class we need and make an instance."""
    logging.debug(
        "Adding a forecast to temporary list: {}, {}, {}, {}, {}".format(
            event_start, belief_time, source, sensor, event_value
        )
    )
    if get_config("PERSISTENCE", "type") == "file":
        tb_class = TimedBelief
    else:
        tb_class = DBTimedBelief
    forecast_list.append(
        tb_class(
            event_start=event_start,
            belief_time=belief_time,
            source=source,
            sensor=sensor,
            value=event_value,
        )
    )
