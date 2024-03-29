from typing import Any, Optional, Tuple, Union, List, Callable
from datetime import timedelta, datetime

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Column, Float, String
from sqlalchemy.orm import sessionmaker

from weatherforecast.utils import get_config
from timely_beliefs import DBBeliefSource, DBSensor, DBTimedBelief
from timely_beliefs.db_base import Base as TBBase
from weatherforecast.utils.Sensor import SensorName


SessionClass = sessionmaker()
session = None


class DBLocatedSensor(DBSensor):
    """A sensor with a location lat/long and location name"""

    latitude = Column(Float(), nullable=False)
    longitude = Column(Float(), nullable=False)
    location_name = Column(String(80), nullable=False)

    def __init__(
        self,
        latitude: float = None,
        longitude: float = None,
        location_name: str = "",
        **kwargs
    ):
        self.latitude = latitude
        self.longitude = longitude
        self.location_name = location_name
        DBSensor.__init__(self, **kwargs)

    def __repr__(self):
        return "<DBLocatedSensor: %s (%s) at (%.2f|%.2f:%s)>" % (
            self.id,
            self.name,
            self.latitude,
            self.longitude,
            self.location_name,
        )


def create_db_and_session():
    db_connection_string = get_config("PERSISTENCE", "name")
    print(
        "[WeatherForecasting] Preparing database session for %s" % db_connection_string
    )
    global SessionClass, session

    engine = create_engine(db_connection_string)
    SessionClass.configure(bind=engine)

    TBBase.metadata.create_all(engine)

    if session is None:
        session = SessionClass()

    create_darksky_source(session)

    return session


def create_darksky_source(session):
    source = session.query(DBBeliefSource).first()
    if source is None:
        print("Creating DarkSky source ...")
        session.add(DBBeliefSource(name="DarkSky"))
        session.commit()


def get_or_create_sensors_for(
    session, latitude, longitude, location_name
) -> List[DBLocatedSensor]:
    sensors = []
    added_sensors_to_db = False
    for sensor_name in SensorName.ALL.value:
        db_sensor = (
            session.query(DBLocatedSensor)
            .filter_by(name=sensor_name, latitude=latitude, longitude=longitude)
            .first()
        )
        if db_sensor is not None:
            sensors.append(db_sensor)
        else:
            print("Creating sensor %s at %s ..." % (sensor_name, location_name))
            new_sensor = DBLocatedSensor(
                name=sensor_name,
                latitude=latitude,
                longitude=longitude,
                location_name=location_name,
                event_resolution=timedelta(minutes=15),
            )
            sensors.append(new_sensor)
            session.add(new_sensor)
            added_sensors_to_db = True
    if added_sensors_to_db:
        session.commit()
    return sensors
