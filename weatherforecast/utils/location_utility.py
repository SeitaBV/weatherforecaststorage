from typing import List
import pandas as pd
import webbrowser
import folium
from folium import features
import os
import logging

from weatherforecast.utils import path_to_data, path_to_maps


selected_columns: List[str] = ['continent_name', 'country_name', 'subdivision_1_name', 'city_name', 'time_zone',
                               'is_in_european_union', 'latitude', 'longitude']
cities_df: pd.DataFrame = pd.read_csv('%s/City-geolocation-en-v2.csv' % path_to_data(), usecols=selected_columns)
cities_df[['city_name', 'subdivision_1_name']] = cities_df[['city_name', 'subdivision_1_name']].fillna('')

city_location_columns: List[str] = ['city_name', 'subdivision_1_name', 'country_name', 'latitude', 'longitude']


def _create_location_name_column(df: pd.DataFrame):
    df['location_name'] = df['city_name'] + '_' + df['subdivision_1_name'] + '_' + df['country_name']
    df.drop(['city_name', 'subdivision_1_name', 'country_name'], axis=1, inplace=True)


def get_all_cities_locations() -> pd.DataFrame:
    logging.info("Getting all cities")
    df = cities_df[city_location_columns].copy()
    _create_location_name_column(df)
    return df


def get_cities_locations_by_continent(continent_name: str) -> pd.DataFrame:
    logging.info("Getting all cities in %s", continent_name)
    df = cities_df.query('continent_name == @continent_name')[city_location_columns].copy().reset_index(drop=True)
    _create_location_name_column(df)
    return df


def get_cities_locations_by_country(country_name: str) -> pd.DataFrame:
    logging.info("Getting all cities in %s", country_name)
    df = cities_df.query('country_name == @country_name')[city_location_columns].copy().reset_index(drop=True)
    _create_location_name_column(df)
    return df


def get_cities_locations_by_subdivision(subdivision_name: str) -> pd.DataFrame:
    logging.info("Getting all cities in %s", subdivision_name)
    df = cities_df.query('subdivision_1_name == @subdivision_name')[city_location_columns].copy().reset_index(
        drop=True)
    _create_location_name_column(df)
    return df


def get_cities_locations_by_time_zone(time_zone: str) -> pd.DataFrame:
    logging.info("Getting all cities in %s", time_zone)
    df = cities_df.query('time_zone == @time_zone')[city_location_columns].copy().reset_index(drop=True)
    _create_location_name_column(df)
    return df


def get_cities_locations_in_european_union() -> pd.DataFrame:
    logging.info("Getting all cities in the european union")
    df = cities_df.query('is_in_european_union == "1"')[city_location_columns].copy().reset_index(drop=True)
    _create_location_name_column(df)
    return df


def get_city_location(city_name: str, country_name: str, subdivision_name: str = None) -> pd.DataFrame:
    logging.info("Getting %s %s", city_name, country_name)
    if subdivision_name is None:
        query = 'city_name == @city_name & country_name == @country_name'
    else:
        query = 'city_name == @city_name & subdivision_1_name == @subdivision_name & country_name == @country_name'

    df = cities_df.query(query)[city_location_columns].copy().reset_index(drop=True)
    _create_location_name_column(df)

    return df


def plot_locations_on_map(file_name: str, locations: pd.DataFrame):
    logging.info("Plotting locations on map")
    file_name += '.html'
    start_location = locations.iloc[0, :]
    leaflet_map = folium.Map([start_location['latitude'], start_location['longitude']], zoom_start=7)
    for location in locations.itertuples():
        latitude = location[1]
        longitude = location[2]
        location_name = location[3]
        marker = features.Marker([latitude, longitude])
        popup = folium.Popup(location_name)
        icon = features.Icon(color='blue')

        marker.add_child(icon)
        marker.add_child(popup)
        leaflet_map.add_child(marker)

    leaflet_map.save(os.path.join(path_to_maps(), file_name))
    webbrowser.open_new_tab(os.path.join(path_to_maps(), file_name))
