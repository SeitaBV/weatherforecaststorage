# WeatherForecastStorage

A tool to collect weather forecasts from Dark Sky and store them in an SQL database.

1. cp config/configuration.ini.sample config/configuration.ini
2. Adjust database connection string in config/configuration.ini
3. Also adjust which cities you want to get forecasts for (an extensive list of cities worldwide is available in data/City_geolocation-en.csv)
4. Run `python get_new_forecasts.py`
