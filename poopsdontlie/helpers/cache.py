import functools
import pandas as pd
import pickle
import urllib.parse
import inspect

from poopsdontlie.helpers import config
from abc import ABCMeta, abstractmethod
from pathlib import Path
from datetime import datetime


_levels_definition = {
    'backend': {'namespace': 'backend'},
    'apiresult': {'namespace': 'api'},
}
levels = [*_levels_definition.keys()]


def local_tz_now():
    tznow = datetime.now().astimezone()

    return pd.Timestamp.utcnow().tz_convert(tznow.tzinfo)


def invalidate_beginning_of_next_month():
    return (local_tz_now() + pd.offsets.MonthBegin(1)).replace(microsecond=0, hour=0, minute=0, second=0).tz_convert('UTC')


def _prep_time_tuple(time_tuple):
    if len(time_tuple) > 3 or len(time_tuple) <= 0:
        raise ValueError(f'time_tuple len should be min. 1, max. 3: (hour, minute, seconds), current values: ({", ".join(time_tuple)})')

    vals = dict(zip(['hour', 'minute', 'second'], time_tuple))

    if 'minute' not in vals:
        vals['minute'] = 0

    if 'second' not in vals:
        vals['second'] = 0

    return vals


def invalidate_after_time_for_tz(time_tuple, tz):
    vals = _prep_time_tuple(time_tuple)

    now_localized = pd.Timestamp.utcnow().tz_convert(tz)
    invalidate_after_localized = pd.Timestamp.utcnow().tz_convert(tz).replace(microsecond=0, **vals)

    if invalidate_after_localized < now_localized:
        return (invalidate_after_localized + pd.Timedelta(days=1)).tz_convert('UTC')

    return invalidate_after_localized.tz_convert('UTC')


def invalidate_tomorrow_after_time_for_tz(time_tuple, tz):
    vals = _prep_time_tuple(time_tuple)

    return (pd.Timestamp.utcnow().tz_convert(tz) + pd.Timedelta(days=1)).replace(microsecond=0, **vals).tz_convert('UTC')


def _is_valid_cache_level(level):
    return level in levels


def cached_results(key, invalidate_after, cache_level='backend'):
    if not _is_valid_cache_level(cache_level):
        raise ValueError(f'Cache level {cache_level} invalid, should be one of {", ".join(levels)}')

    def decorator_cached_results(func):
        _invalidate_registry[func] = {
            'key': key,
            'cache_level': cache_level,
            'invalidate_after': invalidate_after,
        }

        @functools.wraps(func)
        def wrapper_cached_results(*args, **kwargs):
            cache = _cache_factory()

            if cache.exists(key, cache_level):
                retval = cache.get(key, cache_level)
                if retval is not None:
                    print(f'Using cached {key}')
                    return retval

            retval = func(*args, **kwargs)
            cache.put(key, retval, cache_level, invalidate_after)

            return retval
        return wrapper_cached_results
    return decorator_cached_results


class CacheAdapter(metaclass=ABCMeta):
    @abstractmethod
    def put(self, key, value, cache_level, invalidate_by=None):
        pass

    @abstractmethod
    def get(self, key, cache_level):
        pass

    @abstractmethod
    def exists(self, key, cache_level):
        pass

    @abstractmethod
    def remove(self, key, cache_level):
        pass


class NoCache(CacheAdapter):
    def exists(self, key, cache_level):
        return False

    def put(self, key, value, cache_level, invalidate_by=None):
        return None

    def get(self, key, cache_level):
        return None

    def remove(self, key, cache_level):
        return None


class RemoteCache(CacheAdapter):
    def __init__(self, cache_root_url=config['remote_cache_url']):
        self._root_url = cache_root_url
        if self._root_url[-1] != '/':
            self._root_url = f'{self._root_url}/'

    def put(self, key, value, cache_level, invalidate_by=None):
        # unsupported, this cache is read-only
        pass

    def get(self, key, cache_level):
        func, entry = _get_registry_entry_for_key_cache_level(key, cache_level)
        module = inspect.getmodule(func).__name__.split('.')
        country = module[module.index('countries') + 1].upper()

        meta_url = f'{self._root_url}{country}/{func.__name__}.meta'
        csv_url = f'{self._root_url}{country}/{func.__name__}.csv'

    def exists(self, key, cache_level):
        pass

    def remove(self, key, cache_level):
        # unsupported, this cache is read-only
        pass


class LocalFilesystemCache(CacheAdapter):
    def exists(self, key, cache_level):
        cachefile = self._genpath(key, cache_level)

        return cachefile.is_file()

    def __init__(self, cache_dir=Path(config['cachedir'])):
        self._cdir = cache_dir
        self._cdir.mkdir(parents=True, exist_ok=True)

    def _quote_safe(self, str):
        return urllib.parse.quote(str, safe='')

    def _genpath(self, key, cache_level):
        return self._cdir / f'{cache_level}-{self._quote_safe(key)}.bin'

    def _read(self, path):
        with open(path, 'rb') as fh:
            return pickle.load(fh)

    def _write(self, path, obj):
        with open(path, 'wb') as fh:
            pickle.dump(obj, fh)

    def put(self, key, value, cache_level, invalidate_by=None):
        cachefile = self._genpath(key, cache_level)

        cacheobj = {
            'invalidate_by': invalidate_by,
            'created': pd.Timestamp.utcnow(),
            'value': value,
        }

        self._write(cachefile, cacheobj)

    def get(self, key, cache_level):
        cachefile = self._genpath(key, cache_level)

        if not self.exists(key, cache_level):
            return None

        cacheobj = self._read(cachefile)

        if cacheobj['invalidate_by'] is None or pd.Timestamp.utcnow() < cacheobj['invalidate_by']:
            return cacheobj['value']

        self.remove(key, cache_level)
        return None

    def remove(self, key, cache_level):
        cachefile = self._genpath(key, cache_level)
        if cachefile.is_file():
            cachefile.unlink()


def _cache_factory():
    if hasattr(_cache_factory, '_instance'):
        if config['cache'] == _cache_factory._impl:
            return _cache_factory._instance

    cache_impl = config['cache']
    if cache_impl is None or cache_impl == '' or cache_impl.lower().strip() == 'none':
        return NoCache()

    cache_impl = cache_impl.lower().strip()
    cache = None
    if cache_impl == 'local':
        cache = LocalFilesystemCache()

    if cache is None:
        raise ValueError(f'Invalid cache in config: {cache_impl}, try one of: remote, local, none')

    _cache_factory._impl = config['cache']
    _cache_factory._instance = cache

    return cache


def _get_registry_entry_for_key_cache_level(key, cache_level):
    for k, v in _invalidate_registry.items():
        if v['key'] == key and v['cache_level'] == cache_level:
            return k, _invalidate_registry[k]


def get_func_invalidate_after(func):
    if isinstance(func, str):
        for k in _invalidate_registry:
            if k.__name__ == func:
                func = k
                break

    if func in _invalidate_registry:
        return _invalidate_registry[func]['invalidate_after']


_invalidate_registry = {}
