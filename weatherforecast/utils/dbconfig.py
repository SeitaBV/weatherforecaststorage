from typing import Any, Optional, Tuple, Union, List, Callable
from datetime import timedelta, datetime

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Column, Float, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

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
        name: str = "",
        unit: str = "",
        timezone: str = "UTC",
        latitude: float = None,
        longitude: float = None,
        location_name: str = "",
        event_resolution: Optional[timedelta] = None,
        knowledge_horizon: Optional[
            Union[timedelta, Tuple[Callable[[datetime, Any], timedelta], dict]]
        ] = None,
    ):
        self.latitude = latitude
        self.longitude = longitude
        self.location_name = location_name
        DBSensor.__init__(
            self, name, unit, timezone, event_resolution, knowledge_horizon
        )
        TBBase.__init__(self)

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


'''
db = None
Base = None
session_options = None

def init_db():
    """Initialise the database object"""
    global db, Base, session_options
    db = SQLAlchemy(session_options=session_options)
    Base = declarative_base()
    Base.query = None


def configure_db():
    """Call this to configure the database.
    This should only be called once in the app's lifetime."""
    global db, Base

    Base.query = db.session.query_property()

    # Import all modules here that might define models so that
    # they will be registered properly on the metadata. Otherwise
    # you will have to import them first before calling configure_db().
    from bvp.data.models import (  # noqa: F401
        assets,
        data_sources,
        markets,
        weather,
        user,
        task_runs,
        forecasting,
    )  # noqa: F401

    Base.metadata.create_all(bind=db.engine)
'''
