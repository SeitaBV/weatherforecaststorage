from typing import List, Union
import os
import logging
from datetime import datetime, timedelta

import pandas as pd
from timely_beliefs import TimedBelief, DBTimedBelief
from sqlalchemy.orm import Session

from weatherforecast.utils import (
    path_to_data,
    location_utility,
    get_config,
    cols as df_cols,
)
from weatherforecast.utils.Sensor import SensorName
from weatherforecast.utils.weather_forecast_utility import create_forecasts
from weatherforecast.utils.dbconfig import create_db_and_session

"""
This module can be used to get new forecasts from DarkSkye and add them to either a csv file
(mostly useful for testing, probably) or a database.
You'd probably call this once per hour, which is DarkSkye's resolution.
"""


def forecast_is_new(store: Union[Session, pd.DataFrame], fc: TimedBelief) -> bool:
    """Find out if this specific forecast has already been added.
    We take the belief horizon into account. We only want to compare to the latest relevant belief.
    For instance, we believed x, later y and even later x again. That is all valuable knowledge.
    Should be rare, as beliefs usually become more accurate over time, but still.
    """
    persistence_type = get_config("PERSISTENCE", "type")
    if persistence_type == "file":  # store is a DataFrame
        if store.empty:
            return True
        query = (  # TODO: only take latest belief into account, see db
            "event_start == @fc.event_start & source == @fc.source.id &"
            " sensor_id == @fc.sensor.id & event_value == @fc.event_value"
        )
        temp = store.query(query)
        return temp.shape[0] == 0
    elif persistence_type == "db":  # store is a db session
        latestRelevantBelief = (
            store.query(DBTimedBelief)
            .filter(
                DBTimedBelief.event_start == fc.event_start,
                DBTimedBelief.source_id == fc.source.id,
                DBTimedBelief.sensor_id == fc.sensor.id,
                DBTimedBelief.belief_horizon != fc.belief_horizon,
            )
            .order_by(DBTimedBelief.belief_horizon.asc())
            .first()
        )
        if latestRelevantBelief is None:
            return True
        return latestRelevantBelief.event_value != fc.event_value
    else:
        raise Exception("Unkown persistence type: %s" % persistence_type)


def filter_out_known_forecast(
    current_forecasts: List[TimedBelief], store: Union[Session, pd.DataFrame]
) -> List[TimedBelief]:
    new_entries_list = []
    for fc in current_forecasts:
        if forecast_is_new(store, fc):
            logging.debug("Adding new forecast: %s" % fc)
            new_entries_list.append(fc)
        else:
            logging.debug("Did not add this new forecast: %s" % fc)
            if get_config("PERSISTENCE", "type") == "db":
                # is already in session because the linked sensor and source are
                store.delete(fc)

    return new_entries_list


def save_forecasts(
    new_entries: List[TimedBelief], store: Union[Session, pd.DataFrame]
) -> Union[Session, pd.DataFrame]:
    """Save forecasts which are deemed novel, to either file or database.
       In the former case, the returned store object has been updated."""
    if len(new_entries) > 0:
        logging.info("Adding {} new entries.".format(len(new_entries)))
        persistence_type = get_config("PERSISTENCE", "type")
        if persistence_type == "file":  # store is a DataFrame
            new_entries_df = pd.DataFrame(
                new_entries, columns=df_cols
            )  # TODO: make work
            store = store.append(new_entries_df, ignore_index=True, sort=True)
            store.to_csv(
                "%s/%s" % (path_to_data(), get_config("PERSISTENCE", "name")),
                index=False,
                columns=new_entries.columns,
            )
        elif persistence_type == "db":  # store is a db session
            for belief in new_entries:
                store.add(belief)
            store.commit()
        else:
            raise Exception("Unkown persistence type: %s" % persistence_type)
    else:
        logging.info("No new entries.")
    return store


if __name__ == "__main__":
    """
    """
    logging.basicConfig(level=logging.INFO)
    sensor_names = SensorName.ALL.value

    store = None  # this determines where we keep forecasts
    if get_config("PERSISTENCE", "type") == "file":
        # reading historical forecasts once, which we will need for file-based storage
        logging.info("Reading in existing forecasts ...")
        if os.path.exists("%s/forecasts.csv" % path_to_data()):
            store = pd.read_csv("%s/forecasts.csv" % path_to_data())
        else:
            store = pd.DataFrame()
    else:
        store = create_db_and_session()

    for forecast_location in get_config("LOCATIONS"):
        city, country = [s.strip() for s in forecast_location.split(",")]
        logging.info("Getting weather forecasts for %s (%s) ..." % (city, country))

        logging.info(
            "Asking DarkSky for forecasts and finding out which ones are novel ..."
        )
        city_locations = location_utility.get_city_location(city, country)
        if city_locations.index.size == 0:
            logging.warning(
                "Location %s,%s cannot be found! Please check data/City_geolocation-en.csv if we map it."
                " Maybe you misspelled it." % (city, country)
            )
            continue
        current_forecasts = create_forecasts(
            city_locations, sensor_names, 12
        )  # TODO: make number of hours configurable
        new_entries = filter_out_known_forecast(current_forecasts, store=store)
        store = save_forecasts(new_entries, store=store)
