import os.path
import configparser


cols = ["event_start", "belief_time", "source", "sensor_id", "event_value"]


def get_config(section: str = None, option: str = None) -> str:
    if section is None:
        raise Exception("Cannot get config when section is None ...")
    conf_file_path = "%s/configuration.ini" % path_to_config()
    if not os.path.exists(conf_file_path):
        raise Exception("Cannot find configuration file at %s" % conf_file_path)
    config = configparser.ConfigParser()
    config.read(conf_file_path)
    if option is None:
        return [v for _, v in config.items(section)]
    return config.get(section, option)


def _path_to_main() -> str:
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "..")


def path_to_data() -> str:
    return os.path.join(_path_to_main(), "data")


def path_to_config() -> str:
    return os.path.join(_path_to_main(), "weatherforecast/config")


def path_to_maps() -> str:
    return os.path.join(_path_to_main(), "leaflet-map")
