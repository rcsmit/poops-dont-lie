from poopsdontlie.countries.nld.helpers import download_sewage_data, get_rwzi_gmvm_mapped_data, rivm_update_time
from poopsdontlie.helpers.cache import cached_results, invalidate_after_time_for_tz
from poopsdontlie.helpers import config
from tqdm.auto import tqdm

import pandas as pd


@cached_results(
    key='rna_flow_per_100k_people_for_veiligheidsregio',
    invalidate_after=invalidate_after_time_for_tz(*rivm_update_time),
    cache_level='apiresult'
)
def rna_flow_per_100k_people_for_veiligheidsregio(jobs=config['n_jobs']):
    df_rwzi_gm_vr = get_rwzi_gmvm_mapped_data(jobs=jobs)
    vrcols = sorted([x for x in df_rwzi_gm_vr.columns if x.startswith('VR')])

    df_vr_rna_flow = pd.DataFrame(index=pd.to_datetime([]))

    print('Converting RNA flow per municipality / safety-region to flow per 100k')
    for col in tqdm(vrcols):
        df_vr = df_rwzi_gm_vr[['Date_measurement', col, 'population_attached_to_rwzi']].groupby('Date_measurement').sum()
        df_vr_rna_flow = df_vr_rna_flow.join(
            (df_vr[col] / df_vr['population_attached_to_rwzi'] * 100_000).round(0).resample('D').last().rename(f'RNA_flow_per_100000_{col}'),
            how='outer'
        )

    return df_vr_rna_flow.round(0).astype(pd.Int64Dtype())


@cached_results(
    key='rna_flow_per_100k_people_for_gemeente',
    invalidate_after=invalidate_after_time_for_tz(*rivm_update_time),
    cache_level='apiresult'
)
def rna_flow_per_100k_people_for_gemeente(jobs=config['n_jobs']):
    df_rwzi_gm_vr = get_rwzi_gmvm_mapped_data(jobs=jobs)
    gmcols = sorted([x for x in df_rwzi_gm_vr.columns if x.startswith('GM')])

    df_gem_rna_flow = pd.DataFrame(index=pd.to_datetime([]))

    print('Converting RNA flow per municipality / safety-region to flow per 100k')
    for col in tqdm(gmcols):
        df_gem = df_rwzi_gm_vr[['Date_measurement', col, 'population_attached_to_rwzi']].groupby('Date_measurement').sum()
        df_gem_rna_flow = df_gem_rna_flow.join(
            (df_gem[col] / df_gem['population_attached_to_rwzi'] * 100_000).round(0).resample('D').last().rename(f'RNA_flow_per_100000_{col}'),
            how='outer'
        )

    return df_gem_rna_flow.round(0).astype(pd.Int64Dtype())


@cached_results(
    key='rna_flow_per_100k_people_for_rwzi',
    invalidate_after=invalidate_after_time_for_tz(*rivm_update_time),
    cache_level='apiresult'
)
def rna_flow_per_100k_people_for_rwzi():
    return download_sewage_data()
