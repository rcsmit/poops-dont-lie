from setuptools import setup, find_packages
from setuptools.command.egg_info import egg_info

import sys


class EggInfoEx(egg_info):
    """Includes license file into `.egg-info` folder."""

    def run(self):
        # don't duplicate license into `.egg-info` when building a distribution
        if not self.distribution.have_run.get('install', True):
            # `install` command is in progress, copy license
            self.mkpath(self.egg_info)
            self.copy_file('LICENSE', self.egg_info)

        egg_info.run(self)


if sys.version_info < (3, 6):
    sys.exit('Sorry, Python < 3.6 is not supported')

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='poops-dont-lie',
    version='1',
    license_files = ('LICENSE', ),
    cmdclass = {'egg_info': EggInfoEx},
    description='COVID-19 RNA flow per ML of sewage per 100k population dataset',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Thomas Phil',
    author_email='thomas@tphil.nl',
    url='https://github.com/Sikerdebaard/poopsdontlie',
    python_requires=">=3.6",
    packages=find_packages(),  # same as name
    install_requires=[
        'joblib>=1.1.0',
        'psutil>=5.5.1',
        'numpy>=1.22.3',
        'pandas>=1.4.2',
        'openpyxl>=3.0.9',
        'tqdm>=4.64.0',
        'requests>=2.22.0',
        'pyyaml>=5.3.1',
        'appdirs>=1.4.4',
        'cleo>=0.8.1',
        'pycountry>=22.3.5',
        'statsmodels>=0.13.2',
    ],
    entry_points={
        'console_scripts': [
            'poopsdontlie=poopsdontlie.cli.poopsdontlie:run',
        ],
    },
)
