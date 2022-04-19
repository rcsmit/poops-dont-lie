#!/usr/bin/env python
from cleo import Command, Application
from poopsdontlie.api import list_countries, list_country_regions, is_valid_region, get_region_data_for_country, get_valid_regions
from poopsdontlie.helpers.config import config, config_file, write_default_config
from pathlib import Path

import os
import logging
import pkg_resources  # part of setuptools

from poopsdontlie.helpers.remotecache import cache_gen


class Config(Command):
    """
    Shows the apps config

    config
        {--refresh : Replace current config with default config}
    """
    def handle(self):  # type: () -> Optional[int]
        self.write(f'Config location: {config_file.absolute()}\n\n')

        if self.option('refresh'):
            write_default_config()

        with open(config_file, 'r') as fh:
            print(fh.read())


class GenerateRemoteCache(Command):
    """
    This command generates the cache that is maintained at https://github.com/Sikerdebaard/poops-dont-lie-data.

    cache-gen
        {outdir : Output path where the csv and meta files are stored.}
        {--no-cache : Do not use cache}
        {--force-regen : Force regenerating all cache files}
    """

    def handle(self):  # type: () -> Optional[int]
        outdir = Path(self.argument('outdir'))

        outdir.mkdir(exist_ok=True, parents=True)

        if self.option('no-cache'):
            config['cache'] = None

        force_regen = False
        if self.option('force-regen'):
            force_regen = True

        cache_gen(outdir, force_regen)


class ListSupportedCountries(Command):
    """
    Shows a list of countries with a supported waste water dataset

    list
    """
    def handle(self):  # type: () -> Optional[int]
        for iso, country in list_countries().items():
            regions = list_country_regions(country)

            for region in regions:
                print(region)


class ListSupportedRegions(Command):
    """
    Shows a list of supported regions for a country with a supported waste water dataset

    regions
        {country : ISO Alpha-3 name of the country as listed by the list-command}
    """

    def handle(self):  # type: () -> Optional[int]
        country = self.argument('country')

        valid_countries = sorted(list_countries().keys())

        if country is None or len(country) != 3 or country.upper() not in valid_countries:
            self.line(f'<error>Error:</error> country {country} not supported, use one of: {", ".join(valid_countries)}')
            return

        regions = list_country_regions(country)

        for region in regions:
            print(region)


class GetRegionData(Command):
    """
    Get the wastewater dataset for a specific country / region pair.

    get
        {country : ISO Alpha-3 name of the country as listed by the list-command}
        {region : Region name of the country as listed by the regions-command}
        {--no-cache : Do not use cache}
        {--cache-type= : Override config cache type, choose one of remote, local, none}
        {--c|cache-dir= : Set cache dir for local cache}
    """

    def handle(self):  # type: () -> Optional[int]
        country = self.argument('country')
        region = self.argument('region')

        if self.option('cache-type'):
            cache_type = self.option('cache-type').lower().strip()
            valid_types = ['remote', 'local', 'none']
            if cache_type not in valid_types:
                self.write(f'<error>Error:</error> --cache-type={cache_type} invalid, choose one of: {", ".join(valid_types)}')

        if self.option('no-cache'):
            config['cache'] = None

        if self.option('cache-dir'):
            config['cachedir'] = self.option('cache-dir')

        valid_countries = sorted(list_countries().keys())

        if country is None or len(country) != 3 or country.upper() not in valid_countries:
            self.line(f'<error>Error:</error> country {country} not supported, use one of: {", ".join(valid_countries)}')
            return

        if not is_valid_region(country, region):
            self.line(f'<error>Error:</error> region {region} not supported, use one of: {", ".join(get_valid_regions(country))}')

        print(get_region_data_for_country(country, region))


def run():
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    package = 'poops-dont-lie'
    ver = pkg_resources.require(package)[0].version

    application = Application(name=package, version=ver)

    if "ENABLE_ADMIN" in os.environ:
        application.add(GenerateRemoteCache())

    application.add(Config())
    application.add(ListSupportedCountries())
    application.add(ListSupportedRegions())
    application.add(GetRegionData())

    application.run()


if __name__ == '__main__':
    run()
