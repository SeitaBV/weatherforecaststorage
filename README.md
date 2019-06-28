# WeatherForecastStorage

A tool to collect weather forecasts from DarkSky for any number of cities in the world and store them in an SQL database.
This is useful to run as an hourly (cron) job.

DarkSky provides forecasts for many kinds of sensors (e.g. temperature, wind speed) up to 24 hours ahead.
However, they do not store these forecasts for later retrieval. For past data, you'll only get actual measurements from their API. To train and test any forecasting model, you'll need actual forecasts and this tool helps you automate their collection.

We save the forecasts based on a data model provided by [the TimleyBeliefs package](https://github.com/SeitaBV/timely-beliefs). This model connects sensors (which are automatically created for you) to forecasts (beliefs), and keeps track of event time and belief time.


## Setup

0. (optional) install / activate a virtual environment
1. `export PYTHONPATH=/your/path/to/weatherforecaststorage`
2. `pip install -r requirements.txt`
3. `cp config/configuration.ini.sample config/configuration.ini`
4. Adjust database connection string in config/configuration.ini
5. Also adjust which cities you want to get forecasts for (an extensive list of cities worldwide is available in data/City_geolocation-en.csv)
6. Create a sensor/location lookup table:
    `python -c "from weatherforecast.utils import helping_tables_utility; helping_tables_utility.create_sensor_location_id_mapping_table()"`

## Generate forecasts

Run this command to generate forecasts:
    
    python weatherforecast/get_new_forecasts.py
