# WeatherForecastStorage

A tool to collect weather forecasts from Dark Sky and store them in an SQL database.

1. export PYTHONPATH=/path/to/weatherforecaststorage
2. pip install -r requirements.txt
3. cp config/configuration.ini.sample config/configuration.ini
4. Adjust database connection string in config/configuration.ini
5. Also adjust which cities you want to get forecasts for (an extensive list of cities worldwide is available in data/City_geolocation-en.csv)
6. Run `python get_new_forecasts.py`
