import configparser

_CONFIG_PATH = './config/config.ini'
_CONFIG = configparser.ConfigParser()
_CONFIG.read(_CONFIG_PATH)

assert "POSTGRES" in _CONFIG, "config.ini must define postgres connection in a section called [POSTGRES]"

PG_CONFIG = dict(_CONFIG.items("POSTGRES"))

assert "RUN OPTS" in _CONFIG, "config.ini must define run defaults in a section called [RUN OPTS]"

BATCH_SIZE = _CONFIG["RUN OPTS"].getint("batch_size")
DATA_DIR = _CONFIG["RUN OPTS"].get("data_dir")
