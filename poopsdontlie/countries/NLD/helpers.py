from poopsdontlie.helpers.io import download_file_with_progressbar
from poopsdontlie.helpers.cache import cached_results, invalidate_beginning_of_next_month, invalidate_after_time_for_tz
from poopsdontlie.helpers.joblib import tqdm_joblib
from joblib import Parallel, delayed
from tqdm.auto import tqdm
from functools import lru_cache

import numpy as np
import pandas as pd
import geopandas as gpd
import warnings


rivm_update_time = [(15, 17), 'Europe/Amsterdam']  # updates start at 15:15 Amsterdam time, it usually takes a minute or two before update is finished

@cached_results(key='cbs_awzi_population_mappings_2020', invalidate_after=invalidate_beginning_of_next_month(), cache_level='backend')
def download_awzi_population_mappings_2020():
    mapping_excel = 'https://www.cbs.nl/-/media/_excel/2021/01/aantal-inwoners-per-verzorgingsgebied-van-rioolwaterzuiveringsinstallaties.xlsx'
    sheet = 'Tabel 1'
    df_rwzi = pd.read_excel(download_file_with_progressbar(mapping_excel), sheet, skiprows=2)

    # both start and end-rows have the same offset
    offset = 3
    df_rwzi = df_rwzi.iloc[offset:-offset]

    idx_geen = (df_rwzi['Code Rioolwaterzuiveringsinstallatie'] == 'Geen').idxmax()
    df_rwzi.at[idx_geen, 'Code Rioolwaterzuiveringsinstallatie'] = np.nan
    df_rwzi['Code Rioolwaterzuiveringsinstallatie'] = df_rwzi['Code Rioolwaterzuiveringsinstallatie'].astype(float)

    return df_rwzi.reset_index(drop=True)


@cached_results(key='cbs_awzi_population_mappings_2021', invalidate_after=invalidate_beginning_of_next_month(), cache_level='backend')
def download_awzi_population_mappings_2021():
    mapping_excel = 'https://www.cbs.nl/-/media/_excel/2021/39/20210930-aantal-inwoners-per-verzorgingsgebied-2021.xlsx'
    sheet = 'Tabel 1'
    df_rwzi = pd.read_excel(download_file_with_progressbar(mapping_excel), sheet)

    return df_rwzi


@cached_results(key='rivm_sewage_data', invalidate_after=invalidate_after_time_for_tz(*rivm_update_time), cache_level='backend')
def download_sewage_data():
    df_sewage = pd.read_json(download_file_with_progressbar('https://data.rivm.nl/covid-19/COVID-19_rioolwaterdata.json'))

    df_sewage['Date_measurement'] = pd.to_datetime(df_sewage['Date_measurement'])
    df_sewage = df_sewage.set_index('Date_measurement')

    df_sewage.sort_index(inplace=True)
    df_sewage['RNA_flow_per_100000'] = df_sewage['RNA_flow_per_100000'].replace('', np.nan).astype(float)

    return df_sewage


def get_vals_for_non_null_cols(cols, df):
    sel = ~df[cols].isnull()
    sel = sel.columns[sel.iloc[0]]
    sel = df[sel]

    return sel


def gm_or_vr_to_dict_2020(df, popsize):
    sel = df.iloc[0]

    # assert percentage of total is no larger than 101%
    # due to rounding errors @ CBS there must be some leeway
    assert sel.sum() <= 101

    return {k.split('\n')[0].split(' ')[0]: int(round(v / 100 * popsize, 0)) for k, v in sel.to_dict().items()}


def get_rwzi_mappings_2020(rwzi_number, df_rwzi_2020, vrcols_2020, gmcols_2020):
    if not hasattr(get_rwzi_mappings_2020, 'cache'):
        get_rwzi_mappings_2020.cache = {}

    if rwzi_number in get_rwzi_mappings_2020.cache:
        return get_rwzi_mappings_2020.cache[rwzi_number]

    df_rwzi = df_rwzi_2020[df_rwzi_2020['Code Rioolwaterzuiveringsinstallatie'] == rwzi_number]

    if df_rwzi.shape[0] == 0:
        return None

    # we only expect one row per rwzi_number
    # aka: rwzi_number should be unique
    # in the 2020 dataset
    assert df_rwzi.shape[0] == 1

    ret = {
        'population_size': df_rwzi['Inwoners verzorgingsgebied'].sum(),
    }

    df_gm = get_vals_for_non_null_cols(gmcols_2020, df_rwzi)
    df_vr = get_vals_for_non_null_cols(vrcols_2020, df_rwzi)

    ret['VR'] = gm_or_vr_to_dict_2020(df_vr, ret['population_size'])
    ret['GM'] = gm_or_vr_to_dict_2020(df_gm, ret['population_size'])

    get_rwzi_mappings_2020.cache[rwzi_number] = ret

    return ret


def get_rwzi_mappings_2021(measurement_date, rwzi_number, df_rwzi_2021):
    df_rwzi = df_rwzi_2021[df_rwzi_2021['rwzi_code'] == rwzi_number]

    if df_rwzi.shape[0] == 0:
        return None

    df_rwzi = df_rwzi[(measurement_date >= df_rwzi['startdatum']) &
                      (
                          (measurement_date <= df_rwzi['einddatum']) |
                          (df_rwzi['einddatum'].isnull())
                      )]

    # first select if we take the definitief or voorlopig number
    # due to the sort_values -> first() the definitief is the
    # preffered value
    sel = df_rwzi[['regio_code', 'toelichting']].reset_index().sort_values(['regio_code', 'toelichting']).groupby('regio_code').first()
    sel = df_rwzi[df_rwzi.index.isin(sel['index'])].copy()

    sel['aantal'] = (sel['inwoners'] * sel['aandeel']).round(0).astype(int)

    df_vr = sel[sel['regio_type'] == 'VR']
    df_gm = sel[sel['regio_type'] == 'GM']

    # assert percentage of total is between 99% and 101%
    # due to rounding errors @ CBS there must be some leeway
    assert .99 <= df_vr['aandeel'].sum() <= 1.01
    assert .99 <= df_gm['aandeel'].sum() <= 1.01

    ret = {
        'population_size': int(round((df_vr['aantal'].sum() + df_gm['aantal'].sum()) / 2, 0)),
        'GM': {row['regio_code']: row['aantal'] for idx, row in df_gm[['regio_code', 'aantal']].iterrows()},
        'VR': {row['regio_code']: row['aantal'] for idx, row in df_vr[['regio_code', 'aantal']].iterrows()},
    }

    return ret


def get_rwzi_mappings(measurement_date, rwzi_number, idx, df_rwzi_2020, vrcols_2020, gmcols_2020, df_rwzi_2021):
    ret = None

    if measurement_date.year == 2020:
        ret = get_rwzi_mappings_2020(rwzi_number, df_rwzi_2020, vrcols_2020, gmcols_2020)
    elif measurement_date.year > 2020:
        ret = get_rwzi_mappings_2021(measurement_date, rwzi_number, df_rwzi_2021)

        # if it doesn't exist, it's probably in the 2020 dataset
        if ret is None:
            ret = get_rwzi_mappings_2020(rwzi_number, df_rwzi_2020, vrcols_2020, gmcols_2020)

    if ret is not None:
        ret['idx'] = idx

    return ret


def _rwzi_mappings_worker(rows, df_rwzi_2020, vrcols_2020, gmcols_2020, df_rwzi_2021):
    retvals = []
    for idx, row in rows.iterrows():
        retvals.append(get_rwzi_mappings(row['Date_measurement'], row['RWZI_AWZI_code'], idx, df_rwzi_2020, vrcols_2020, gmcols_2020, df_rwzi_2021))

    return retvals


@cached_results(key='merged_mapping_rwzi_gmvr', invalidate_after=invalidate_after_time_for_tz(*rivm_update_time), cache_level='backend')
def map_merge_rwzi_gmvr(df_rwzi_gm_vr, jobs):
    df_rwzi_2021 = get_df_rwzi_2021()
    df_rwzi_2020, vrcols_2020, gmcols_2020 = get_df_rwzi_2020()

    chunksize = 300
    chunks = np.array_split(df_rwzi_gm_vr, np.ceil(df_rwzi_gm_vr.shape[0] / chunksize))

    print('Map rwzi data to municipalities / safety-regions')
    with tqdm_joblib(tqdm(total=len(chunks), unit='runner tasks')) as progress_bar:
        # retvals = Parallel(n_jobs=jobs)(
        #     delayed(get_rwzi_mappings)(row['Date_measurement'], row['RWZI_AWZI_code'], idx, df_rwzi_2020, vrcols_2020, gmcols_2020, df_rwzi_2021) for idx, row in df_rwzi_gm_vr.iterrows()
        # )
        retvals = Parallel(n_jobs=jobs)(
            delayed(_rwzi_mappings_worker)(rows, df_rwzi_2020, vrcols_2020, gmcols_2020, df_rwzi_2021) for rows in chunks
        )


    print('Merging mapped results')

    # ignore fragmentation error
    warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
    for i in tqdm(retvals):
        for r in i:
            assert r is not None

            idx = r['idx']
            df_rwzi_gm_vr.at[idx, 'population_attached_to_rwzi'] = r['population_size']

            for k, v in r['GM'].items():
                df_rwzi_gm_vr.at[idx, k] = v

            for k, v in r['VR'].items():
                df_rwzi_gm_vr.at[idx, k] = v

    # defrag the table
    df_rwzi_gm_vr = df_rwzi_gm_vr.copy()

    # enable performance warnings
    warnings.simplefilter(action='default', category=pd.errors.PerformanceWarning)

    return df_rwzi_gm_vr


@cached_results(key='rna_flow_per_gmvr', invalidate_after=invalidate_after_time_for_tz(*rivm_update_time), cache_level='backend')
def rna_flow_per_gmvr(df_rna_flow_gmvz, gmcols, vrcols):
    print('Splitting RNA flow per municipality / safety-region')
    for col in tqdm([*gmcols, *vrcols]):
        df_rna_flow_gmvz[col] = df_rna_flow_gmvz['RNA_flow_per_100000'] / 100_000 * df_rna_flow_gmvz['population_attached_to_rwzi'] * (
            df_rna_flow_gmvz[col] / df_rna_flow_gmvz['population_attached_to_rwzi'])

    cast_cols = ['RNA_flow_per_100000', 'population_attached_to_rwzi', *gmcols, *vrcols]

    print('Casting float64 -> int64')
    for col in tqdm(cast_cols):
        df_rna_flow_gmvz[col] = df_rna_flow_gmvz[col].round(0).astype(pd.Int64Dtype())

    return df_rna_flow_gmvz


@cached_results(key='get_rwzi_gmvm_mapped_data', invalidate_after=invalidate_after_time_for_tz(*rivm_update_time), cache_level='backend')
def get_rwzi_gmvm_mapped_data(jobs):
    df_sewage = download_sewage_data()

    df_rwzi_gm_vr = df_sewage[['RWZI_AWZI_code', 'RWZI_AWZI_name', 'RNA_flow_per_100000']].reset_index()
    df_rwzi_gm_vr = map_merge_rwzi_gmvr(df_rwzi_gm_vr, jobs)

    gmcols = sorted([x for x in df_rwzi_gm_vr.columns if x.startswith('GM')])
    vrcols = sorted([x for x in df_rwzi_gm_vr.columns if x.startswith('VR')])

    cols_sorted = ['Date_measurement', 'RWZI_AWZI_code', 'RWZI_AWZI_name', 'RNA_flow_per_100000', 'population_attached_to_rwzi', *gmcols, *vrcols]
    df_rwzi_gm_vr = df_rwzi_gm_vr[cols_sorted]

    df_rwzi_gm_vr = rna_flow_per_gmvr(df_rwzi_gm_vr, gmcols, vrcols)

    return df_rwzi_gm_vr


@lru_cache()
def get_df_rwzi_2020():
    df_rwzi_2020 = download_awzi_population_mappings_2020()

    vrcols_2020 = [x for x in df_rwzi_2020.columns if x.upper().startswith('VR')]
    gmcols_2020 = [x for x in df_rwzi_2020.columns if x.upper().startswith('GM')]

    return df_rwzi_2020, vrcols_2020, gmcols_2020


@lru_cache()
def get_df_rwzi_2021():
    df_rwzi_2021 = download_awzi_population_mappings_2021()

    return df_rwzi_2021


@cached_results(key='get_geodata_gemeentes', invalidate_after=invalidate_beginning_of_next_month(), cache_level='backend')
def get_geodata_gemeentes():
    # Haal de kaart met gemeentegrenzen op van PDOK
    geodata_url = 'https://geodata.nationaalgeoregister.nl/cbsgebiedsindelingen/wfs?request=GetFeature&service=WFS&version=2.0.0&typeName=cbs_gemeente_2021_gegeneraliseerd&outputFormat=json'
    df_gemeentegrenzen = gpd.read_file(geodata_url)

    return df_gemeentegrenzen
