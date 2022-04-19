from poopsdontlie.countries import countries
from functools import lru_cache

import pycountry


def list_countries():
    return {k: pycountry.countries.get(alpha_3=k) for k in list(sorted(countries.keys()))}


def list_country_regions(country):
    return [f'{" / ".join(k)} - {v[0]}' for k, v in countries[country].regions.items()]


@lru_cache(maxsize=None)
def _regionmap(country):
    regionmap = {}
    for k, v in countries[country].regions.items():
        for s in k:
            regionmap[s.lower()] = v[1]

    return regionmap

def get_all_region_data_funcs_for_country(country):
    for k, v in countries[country].regions.items():
        yield v[1].__name__, v[1]

def is_valid_region(country, region):
    return region.lower() in _regionmap(country).keys()


def get_valid_regions(country):
    return _regionmap(country).keys()


def get_region_data_for_country(country, region):
    regionmap = _regionmap(country)

    return regionmap[region.lower()]()
