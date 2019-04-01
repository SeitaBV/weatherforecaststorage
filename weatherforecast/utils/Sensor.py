from enum import Enum


class Sensor(Enum):
    ALL = ['precipIntensity', 'precipProbability', 'temperature',
           'apparentTemperature', 'dewPoint', 'humidity', 'pressure',
           'windSpeed', 'windGust', 'windBearing', 'cloudCover',
           'uvIndex', 'visibility', 'ozone']
    PRECIP_INTENSITY = 'precipIntensity'
    PRECIP_PROBABILITY = 'precipProbability'
    TEMPERATURE = 'temperature'
    APPARENT_TEMPERATURE = 'apparentTemperature'
    DEW_POINT = 'dewPoint'
    HUMIDITY = 'humidity'
    PRESSURE = 'pressure'
    WIND_SPEED = 'windSpeed'
    WIND_GUST = 'windGust'
    WING_BEARING = 'windBearing'
    CLOUD_COVER = 'cloudCover'
    UV_INDEX = 'uvIndex'
    VISIBILITY = 'visibility'
    O_ZONE = 'ozone'
