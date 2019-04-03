from weatherforecast.utils import location_utility
from weatherforecast.utils.Sensor import Sensor
from weatherforecast.utils.weather_forecast_utility import create_forecast_table
import logging

if __name__ == '__main__':
    # logging.basicConfig(filename='weather_forecast.log', level=logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG)
    city = 'Amsterdam'
    locations = location_utility.get_city_location('Amsterdam', 'Netherlands')
    sensors = Sensor.ALL.value

    forecast_df = create_forecast_table(locations, sensors)
    print(forecast_df.head())

    location_utility.plot_locations_on_map(city, locations)
