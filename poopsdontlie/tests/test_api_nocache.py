from poopsdontlie.api.countries import get_all_region_data_funcs_for_country, list_countries
from poopsdontlie.helpers import config
from poopsdontlie.helpers.cache import get_func_invalidate_after


# def test_all_country_data():
#     config['cache'] = None
#
#     summary = ''
#
#     for iso, country in list_countries().items():
#         summary += f'##########################\n{iso}: {country.name}\n##########################\n'
#
#         print(f'##########################\nWorking on {iso}: {country.name}\n##########################\n')
#         for name, func in get_all_region_data_funcs_for_country(iso):
#             df = func()
#             print(df)
#             summary += f'{name} invalidates after {get_func_invalidate_after(name)}\n'
#
#     print(summary)
