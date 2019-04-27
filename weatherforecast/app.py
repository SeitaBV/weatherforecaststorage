from weatherforecast.utils import location_utility
from weatherforecast.utils.Sensor import Sensor
from weatherforecast.utils.optimal_locations_finder import OptimalLocationsFinder
from weatherforecast.utils.weather_forecast_utility import create_forecast_table
import logging

if __name__ == '__main__':
    # logging.basicConfig(filename='weather_forecast.log', level=logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG)
    location = 'Netherlands'
    locations = location_utility.get_cities_locations_by_country(location)
    sensors = Sensor.ALL.value

    # forecast_df = create_forecast_table(locations, sensors, overwrite_forecast_file=True)
    # print(forecast_df.head())

    logging.info('Number of locations in {}: {}'.format(location, locations.shape[0]))
    optimal_locations_finder = OptimalLocationsFinder(locations, 60)
    optimal_locations = optimal_locations_finder.find_optimal_locations()

    location_utility.plot_locations_on_map(location, optimal_locations)
    # location_utility.plot_locations_on_map(location, locations)
