import pickle
import pandas as pd

from poopsdontlie import list_countries, get_all_region_data_funcs_for_country
from poopsdontlie.helpers.cache import get_func_invalidate_after


def cache_gen(outdir, force_all=False):
    summary = ''
    mappings = {}

    for iso, country in list_countries().items():
        summary += f'##########################\n{iso}: {country.name}\n##########################\n'

        print(f'##########################\nWorking on {iso}: {country.name}\n##########################\n')
        countrydir = outdir / iso.upper()
        countrydir.mkdir(exist_ok=True, parents=True)
        nowutc = pd.Timestamp.utcnow()
        for name, func in get_all_region_data_funcs_for_country(iso):
            metafile = countrydir / f'{name}.meta'

            if not force_all and metafile.is_file():
                with open(metafile, 'rb') as fh:
                    print(f'Opening existing cache-file {metafile.name}')
                    meta = pickle.load(fh)

                    if meta['invalidate_after'] > nowutc:
                        summary += f'{name} invalidates after {meta["invalidate_after"]} (no change)\n'
                        continue

            df = func()

            df.to_csv(countrydir / f'{name}.csv')
            with open(metafile, 'wb') as fh:
                meta = {
                    'dtypes': df.dtypes.to_dict(),
                    'invalidate_after': get_func_invalidate_after(name),
                }

                pickle.dump(meta, fh, 4)  # format 4 is compatible with all python versions supported by this package

                summary += f'{name} invalidates after {meta["invalidate_after"]}\n'
        summary += '\n'

    print(f'\n\n-------\n\nSUMMARY\n\n-------\n\n{summary}')

    print('DONE')
