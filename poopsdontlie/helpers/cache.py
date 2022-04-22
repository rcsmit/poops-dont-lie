import functools
import inspect

import pandas as pd
import pickle
import urllib.parse

import requests
from requests import HTTPError

from tqdm.auto import tqdm
from poopsdontlie.helpers import config
from abc import ABCMeta, abstractmethod
from pathlib import Path
from datetime import datetime


_levels_definition = {
    'backend': {'namespace': 'backend'},
    'apiresult': {'namespace': 'api'},
    'smoothed_api_result': {'namespace': 'api'}
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


class LocalFilesystemCache(CacheAdapter):
    def exists(self, key, cache_level):
        cachefile = self._genpath(key, cache_level)

        return cachefile.is_file()

    def __init__(self, cache_dir=Path(config['cachedir']) / 'local'):
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


class RemoteCache(CacheAdapter):
    def __init__(self, cache_root_url=config['remote_cache_url'], tmpdir=Path(config['cachedir']) / 'remote'):
        self._root_url = cache_root_url
        if self._root_url[-1] != '/':
            self._root_url = f'{self._root_url}/'

        self._tmpdir = tmpdir

    def _http_get_req_file(self, url, outfile):
        print(f'Downloading {url} to {outfile}')
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                total_size_in_bytes = int(r.headers.get('content-length', 0))
                progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
                with open(outfile, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        # If you have chunk encoded response uncomment if
                        # and set chunk_size parameter to None.
                        # if chunk:
                        progress_bar.update(len(chunk))
                        f.write(chunk)
                progress_bar.close()
        except HTTPError as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(url)
            raise e

    def put(self, key, value, cache_level, invalidate_by=None):
        # unsupported, this cache is read-only
        pass

    def get(self, key, cache_level, ignore_expiredate=False):
        func, entry = _get_registry_entry_for_key_cache_level(key, cache_level)
        module = inspect.getmodule(func).__name__.split('.')
        country = module[module.index('countries') + 1].upper()

        meta_file = f'{func.__name__}.meta'
        csv_file = f'{func.__name__}.csv'

        meta_url = f'{self._root_url}{country}/{meta_file}'
        csv_url = f'{self._root_url}{country}/{csv_file}'

        outpath = self._tmpdir / country
        outpath.mkdir(exist_ok=True, parents=True)

        local_meta_file = outpath / meta_file
        local_csv_file = outpath / csv_file

        try:
            self._http_get_req_file(meta_url, local_meta_file)
        except FileNotFoundError as e:
            # file does not exist in remote cache
            print(f'REMOTE CACHE WARN: {meta_url} does not exist')
            return None

        with open(local_meta_file, 'rb') as fh:
            meta = pickle.load(fh)

        if not ignore_expiredate and meta['invalidate_after'] < pd.Timestamp.utcnow():
            print(f'REMOTE CACHE WARN: {meta["invalidate_after"]} < {pd.Timestamp.utcnow()}')
            return None

        try:
            self._http_get_req_file(csv_url, local_csv_file)
        except FileNotFoundError as e:
            # file does not exist in remote cache
            print(f'REMOTE CACHE WARN: {csv_url} does not exist')
            return None

        dtype, parse_dates = self._filter_dtypes(meta['dtypes'])
        df = pd.read_csv(local_csv_file, index_col=0, dtype=dtype, parse_dates=parse_dates)

        return df

    def _filter_dtypes(self, dtypes):
        typeret = {}
        dateret = []

        for k, v in dtypes.items():
            if 'datetime64[ns]' in str(v):
                dateret.append(k)
            else:
                typeret[k] = v

        return typeret, dateret

    def exists(self, key, cache_level):
        func, entry = _get_registry_entry_for_key_cache_level(key, cache_level)
        module = inspect.getmodule(func).__name__.split('.')
        country = module[module.index('countries') + 1].upper()

        meta_file = f'{func.__name__}.meta'
        meta_url = f'{self._root_url}{country}/{meta_file}'

        try:
            r = requests.head(meta_url)
            r.raise_for_status()
        except HTTPError as e:
            if e.response.status_code == 404:
                return False
            raise e

        return True

    def remove(self, key, cache_level):
        # unsupported, this cache is read-only
        pass



def reiinit_cache_config():
    _cache_factory(force_init=True)


def _cache_factory(force_init=False):
    if not force_init and hasattr(_cache_factory, '_instance'):
        if config['cache'] == _cache_factory._impl:
            return _cache_factory._instance

    cache_impl = config['cache']
    if cache_impl is None or cache_impl == '' or cache_impl.lower().strip() == 'none':
        return NoCache()

    cache_impl = cache_impl.lower().strip()
    cache = None
    if cache_impl == 'local':
        cache = LocalFilesystemCache()
    elif cache_impl == 'remote':
        cache = RemoteCache()

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
