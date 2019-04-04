import os.path


def _path_to_main() -> str:
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "..")


def path_to_data() -> str:
    return os.path.join(_path_to_main(), "data")


def path_to_config() -> str:
    return os.path.join(_path_to_main(), "weatherforecast/config")


def path_to_maps() -> str:
    return os.path.join(_path_to_main(), "leaflet-map")
