import psutil

from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from appdirs import user_cache_dir, user_config_dir
from pathlib import Path

appname = 'poopsdontlie'

default_config = {
    'version': 1,
    'cache': 'local',
    'cachedir': user_cache_dir(appname),
    'remote_cache_url': 'https://github.com/Sikerdebaard/poops-dont-lie-data/raw/main/data/',
    'n_jobs': psutil.cpu_count(),
    'bootstrap_iters': 1_000,
}


config_file = Path(user_config_dir(appname)) / 'config.yml'
config_file.parent.mkdir(parents=True, exist_ok=True)


def write_default_config():
    with open(config_file, 'w') as fh:
        dump(default_config, fh)


if not config_file.is_file():
    write_default_config()

with open(config_file, 'r') as fh:
    config = load(fh, Loader=Loader)

for k, v in default_config.items():
    if k not in config:
        config[k] = v
