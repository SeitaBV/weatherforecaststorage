from enum import Enum
from typing import List

# TODO: we could also list the unit here, e.g. "km/h", "C"


class SensorName(Enum):
    ALL: List[str] = [
        "precipIntensity",
        "precipProbability",
        "temperature",
        "apparentTemperature",
        "dewPoint",
        "humidity",
        "pressure",
        "windSpeed",
        "windGust",
        "windBearing",
        "cloudCover",
        "uvIndex",
        "visibility",
        "ozone",
    ]
    PRECIP_INTENSITY: str = "precipIntensity"
    PRECIP_PROBABILITY: str = "precipProbability"
    TEMPERATURE: str = "temperature"
    APPARENT_TEMPERATURE: str = "apparentTemperature"
    DEW_POINT: str = "dewPoint"
    HUMIDITY: str = "humidity"
    PRESSURE: str = "pressure"
    WIND_SPEED: str = "windSpeed"
    WIND_GUST: str = "windGust"
    WING_BEARING: str = "windBearing"
    CLOUD_COVER: str = "cloudCover"
    UV_INDEX: str = "uvIndex"
    VISIBILITY: str = "visibility"
    O_ZONE: str = "ozone"
