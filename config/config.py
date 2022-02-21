import configparser

_CONFIG_PATH = './config/config.ini'
_CONFIG = configparser.ConfigParser()
_CONFIG.read(_CONFIG_PATH)

assert "POSTGRES" in _CONFIG, "config.ini must define postgres connection in a section called [POSTGRES]"

PG_CONFIG = dict(_CONFIG["POSTGRES"])
