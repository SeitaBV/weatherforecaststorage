import pandas as pd

selected_columns = ['continent_name', 'country_name', 'subdivision_1_name', 'city_name', 'time_zone',
                    'is_in_european_union', 'latitude', 'longitude']
cities_df = pd.read_csv('../data/City-geolocation-en.csv', usecols=selected_columns)

city_location_columns = ['city_name', 'subdivision_1_name', 'country_name', 'latitude', 'longitude']


def _create_location_name_column(df: pd.DataFrame):
    df['location_name'] = df['city_name'] + '_' + df['subdivision_1_name'] + '_' + df['country_name']
    df.drop(['city_name', 'subdivision_1_name', 'country_name'], axis=1, inplace=True)


def get_all_cities_locations():
    df = cities_df[city_location_columns].copy()
    _create_location_name_column(df)
    return df


def get_cities_locations_by_continent(continent_name: str):
    df = cities_df.query('continent_name == @continent_name')[city_location_columns].copy().reset_index(drop=True)
    _create_location_name_column(df)
    return df


def get_cities_locations_by_country(country_name: str):
    df = cities_df.query('country_name == @country_name')[city_location_columns].copy().reset_index(drop=True)
    _create_location_name_column(df)
    return df


def get_cities_locations_by_subdivision(subdivision_name: str):
    df = cities_df.query('subdivision_1_name == @subdivision_name')[city_location_columns].copy().reset_index(
        drop=True)
    _create_location_name_column(df)
    return df


def get_cities_locations_by_time_zone(time_zone: str):
    df = cities_df.query('time_zone == @time_zone')[city_location_columns].copy().reset_index(drop=True)
    _create_location_name_column(df)
    return df


def get_cities_locations_in_european_union():
    df = cities_df.query('is_in_european_union == "1"')[city_location_columns].copy().reset_index(drop=True)
    _create_location_name_column(df)
    return df


def get_city_location(city_name: str, country_name: str, subdivision_name: str = None):
    if subdivision_name is None:
        query = 'city_name == @city_name & country_name == @country_name'
    else:
        query = 'city_name == @city_name & subdivision_1_name == @subdivision_name & country_name == @country_name'

    df = cities_df.query(query)[city_location_columns].copy().reset_index(drop=True)
    _create_location_name_column(df)

    return df
