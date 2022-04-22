import pytest
import random

from poopsdontlie import list_countries, get_all_region_data_funcs_for_country
from poopsdontlie.countries.TEST.mockcountry import emptyfunc
from poopsdontlie.helpers import config
from poopsdontlie.helpers.cache import RemoteCache
from poopsdontlie.helpers.cache import _invalidate_registry, get_func_invalidate_after

cache_test_call_noexpire = {
    'key': 'asd',
    'cache_level': 'zxcvb',
}

# grab a random existing entry out of the cache registry
cache_test_call_exists = {nk: nv for nk, nv in
                          random.choice(list({k: v for k, v in _invalidate_registry.items() if v['cache_level'] == 'apiresult'}.values())).items()
                          if nk in ('key', 'cache_level')
                         }


@pytest.fixture
def remotecache():
    # add some test-entries to invalidate registry

    _invalidate_registry[emptyfunc] = {
        **cache_test_call_noexpire,
        'invalidate_after': None,
    }

    return RemoteCache()


def test_read_file_not_exist(remotecache):
    assert remotecache.get(**cache_test_call_noexpire) is None


def test_read_file_exist(remotecache):
    ret = remotecache.get(**cache_test_call_exists, ignore_expiredate=True)

    assert ret is not None


def test_all_country_data():
    config['cache'] = 'remote'

    summary = ''

    for iso, country in list_countries().items():
        summary += f'##########################\n{iso}: {country.name}\n##########################\n'

        print(f'##########################\nWorking on {iso}: {country.name}\n##########################\n')
        for name, func in get_all_region_data_funcs_for_country(iso):
            df = func()
            assert df is not None

            summary += f'{name} invalidates after {get_func_invalidate_after(name)}\n'

    print(summary)
