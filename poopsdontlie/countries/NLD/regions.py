from poopsdontlie.countries.NLD.helpers import download_sewage_data, get_rwzi_gmvm_mapped_data, rivm_update_time
from poopsdontlie.helpers.cache import cached_results, invalidate_after_time_for_tz
from poopsdontlie.helpers import config
from tqdm.auto import tqdm

import pandas as pd
import numpy as np

from poopsdontlie.smoothers.lowess import lowess


@cached_results(
    key='rna_flow_per_capita_for_veiligheidsregio',
    invalidate_after=invalidate_after_time_for_tz(*rivm_update_time),
    cache_level='apiresult'
)
def rna_flow_per_capita_for_veiligheidsregio(jobs=config['n_jobs']):
    df_rwzi_gm_vr = get_rwzi_gmvm_mapped_data(jobs=jobs)
    vrcols = sorted([x for x in df_rwzi_gm_vr.columns if x.startswith('VR')])

    df_vr_rna_flow = pd.DataFrame(index=pd.to_datetime([]))

    print('Converting RNA flow per municipality / safety-region to flow per capita')
    for col in tqdm(vrcols):
        df_vr = df_rwzi_gm_vr[['Date_measurement', col, 'population_attached_to_rwzi']].groupby('Date_measurement').sum()
        df_vr_rna_flow = df_vr_rna_flow.join(
            (df_vr[col] / df_vr['population_attached_to_rwzi']).round(0).resample('D').last().rename(f'RNA_flow_per_capita_{col}'),
            how='outer'
        )

    return df_vr_rna_flow.round(0).astype(pd.Int64Dtype())


@cached_results(
    key='smoothed_rna_flow_per_capita_for_veiligheidsregio',
    invalidate_after=invalidate_after_time_for_tz(*rivm_update_time),
    cache_level='smoothed_api_result'
)
def smoothed_rna_flow_per_capita_for_veiligheidsregio():
    df = rna_flow_per_capita_for_veiligheidsregio()

    df_smooth = lowess(df, df.columns)

    return df_smooth


@cached_results(
    key='rna_flow_per_capita_for_gemeente',
    invalidate_after=invalidate_after_time_for_tz(*rivm_update_time),
    cache_level='apiresult'
)
def rna_flow_per_capita_for_gemeente(jobs=config['n_jobs']):
    df_rwzi_gm_vr = get_rwzi_gmvm_mapped_data(jobs=jobs)
    gmcols = sorted([x for x in df_rwzi_gm_vr.columns if x.startswith('GM')])

    df_gem_rna_flow = pd.DataFrame(index=pd.to_datetime([]))

    print('Converting RNA flow per municipality / safety-region to flow per capita')
    for col in tqdm(gmcols):
        # first select the columns of interest
        df_sel = df_rwzi_gm_vr[['Date_measurement', col, 'population_attached_to_rwzi']]

        # now divide RNA flow by population number
        df_sel[col] = df_sel[col] / df_sel['population_attached_to_rwzi']

        # now sum RNA flow per day and divide by number of measurements on that day
        df_gem = df_sel.groupby('Date_measurement').sum() / df_sel.groupby('Date_measurement').count()

        # join data into result dataframe
        df_gem_rna_flow = df_gem_rna_flow.join(
            df_gem[col].round(0).resample('D').last().rename(f'RNA_flow_per_capita_{col}'),
            how='outer'
        )

    return df_gem_rna_flow.round(0).replace(0, np.nan).astype(pd.Int64Dtype())


@cached_results(
    key='smoothed_rna_flow_per_capita_for_gemeente',
    invalidate_after=invalidate_after_time_for_tz(*rivm_update_time),
    cache_level='smoothed_api_result'
)
def smoothed_rna_flow_per_capita_for_gemeente():
    df = rna_flow_per_capita_for_gemeente()

    df_smooth = lowess(df, df.columns)

    return df_smooth


@cached_results(
    key='rna_flow_per_capita_for_rwzi',
    invalidate_after=invalidate_after_time_for_tz(*rivm_update_time),
    cache_level='apiresult'
)
def rna_flow_per_capita_for_rwzi():
    df = download_sewage_data()

    df['RNA_flow_per_100000'] = (df['RNA_flow_per_100000'] / 100_000).round(0)

    keep_cols = ['RWZI_AWZI_code', 'RWZI_AWZI_name', 'RNA_flow_per_100000']
    df = df[keep_cols]
    df.rename(columns={'RNA_flow_per_100000': 'RNA_flow_per_capita'}, inplace=True)

    df = df.pivot_table(values='RNA_flow_per_capita', columns='RWZI_AWZI_code', index='Date_measurement')

    df.columns = df.columns.map(lambda x: f'rwzi_awzi_code_{x}')
    df = df.resample('D').last()
    df = df.astype(pd.Int64Dtype())

    return df


@cached_results(
    key='smoothed_rna_flow_per_capita_for_rwzi',
    invalidate_after=invalidate_after_time_for_tz(*rivm_update_time),
    cache_level='smoothed_api_result'
)
def smoothed_rna_flow_per_capita_for_rwzi():
    df = rna_flow_per_capita_for_rwzi()

    df_smooth = lowess(df, df.columns)

    return df_smooth


@cached_results(
    key='rna_flow_per_100k_people_for_rwzi',
    invalidate_after=invalidate_after_time_for_tz(*rivm_update_time),
    cache_level='apiresult'
)
def rna_flow_per_100k_people_for_rwzi():
    return download_sewage_data()
