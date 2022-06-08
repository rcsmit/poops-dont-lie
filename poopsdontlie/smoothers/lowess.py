import random

import pandas as pd
import statsmodels.api as sm
import numpy as np
import scipy.stats as st

from joblib import Parallel, delayed
from tqdm.auto import tqdm
from poopsdontlie.helpers import config
from poopsdontlie.helpers.joblib import tqdm_joblib


# def _numpy_df_resampler(df, replace=True):
#     sample = np.random.choice(df.shape[0], df.shape[0], replace=replace)
#     sampled_df = df.iloc[sample]
#
#     return sampled_df

# def _calc_bootstrap_iter_random_choice(x, y, eval_x, lowess_kw):
#     sample = np.random.choice(len(x), len(x), replace=True)
#     sampled_x = x[sample]
#     sampled_y = y[sample]
#
#     return sm.nonparametric.lowess(exog=sampled_x, endog=sampled_y, xvals=eval_x, **lowess_kw)

def _lowess_on_df(resampled, lowess_kw):
    x = list(range(resampled.shape[0]))
    y = resampled.values.astype(float)

    eval_x = x.copy()

    return sm.nonparametric.lowess(exog=x, endog=y, xvals=eval_x, **lowess_kw)


def _lowess_worker_with_func_resampler(iters, df, func, lowess_kw):
    retvals = []
    for i in range(iters):
        resampled = func(df)

        res = _lowess_on_df(resampled, lowess_kw)

        retvals.append(res)

    return retvals


def _merge_lowess_worker_results(index, retvals):
    df = pd.DataFrame(index=index)
    counter = 0

    for i in range(len(retvals)):
        for j in range(len(retvals[i])):
            df[f'iter_{counter}'] = retvals[i][j]
            df = df.copy()
            counter += 1

    return df


def _bootstrap_quantiles(df_results, conf_interval, bottom_col, top_col):
    bottom, top = np.round((1 - conf_interval) / 2, 4), np.round(conf_interval + (1 - conf_interval) / 2, 4)

    # add 95% CI on quantiles bottom & top from bootstrap resampling
    df_results = df_results.quantile([bottom, top], axis=1).T
    df_results = df_results.rename(columns={bottom: bottom_col, top: top_col})

    return df_results


def _quantile_resampling(df, q=.5, iters=100, replace=True):
    df_res = pd.DataFrame(index=range(df.shape[0]))

    for i in range(iters):
        df_res = pd.concat([df_res, df.sample(frac=1, replace=replace).rename(f'iter_{i}').sort_index().reset_index(drop=True)], axis=1)

    if q is None:
        q = random.random()

    return df_res.quantile(q, axis=1).T


def _bootstrap_ci_from_std(index, bootstrap_metric_std, test_metric, bottom_col, top_col, alpha=0.95):
    """
    Function to calculate confidence interval for bootstrapped samples.
    index: index to be used for the resulting dataframe
    bootstrap_metric_std: numpy array containing the stddev for a metric for the different bootstrap iterations
    test_metric: the value of the metric evaluated on the true, full test set
    alpha: float ranging from 0 to 1 to calculate the alpha*100% CI, default 0.95
    """

    assert len(bootstrap_metric_std) == len(test_metric)

    df_res = pd.DataFrame(index=index)

    result = np.empty((len(bootstrap_metric_std), 2))
    for i in range(len(bootstrap_metric_std)):
        if pd.isna(test_metric[i]):
            result[i, :] = [np.NA, np.NA]

        result[i, :] = st.norm.interval(alpha, loc=test_metric[i], scale=bootstrap_metric_std[i])

    df_res[[bottom_col, top_col]] = result

    return df_res


def lowess_from_median(df, bootstrap_iters=config['bootstrap_iters'], conf_interval=0.95, lowess_kw=None, clip_to_zero=True):
    if lowess_kw is None:
        lowess_kw = {}

    if 'frac' not in lowess_kw:
        frac = np.float64(1) / ((df.index[-1] - df.index[0]) / np.timedelta64(3, 'W'))
        lowess_kw['frac'] = frac

    # use resampling with replacement and calculate the median (quantile=.5)
    resample_lambda = lambda df_l: df_l.sample(frac=1, replace=True, axis=1).quantile(.5, axis=1)

    n_jobs = config['n_jobs']
    remainder = bootstrap_iters % n_jobs
    extra_jobs_mod = n_jobs // remainder
    itersize = bootstrap_iters // n_jobs

    iters = [itersize + (x % extra_jobs_mod == 0) for x in range(n_jobs)]

    with tqdm_joblib(tqdm(total=n_jobs, unit=' bootstrap resampling workers finished', leave=False)) as progress_bar:
        retvals = Parallel(n_jobs=n_jobs)(
            delayed(_lowess_worker_with_func_resampler)(iters[i], df, resample_lambda, lowess_kw) for i in range(n_jobs)
        )

    # calculate the median
    median = _lowess_on_df(df.quantile(.5, axis=1), lowess_kw)

    df_results = _merge_lowess_worker_results(df.index, retvals)

    colnames = {
        'bottom_col': f'median_{conf_interval * 100:0.0f}_perc_ci_bottom',
        'top_col': f'median_{conf_interval * 100:0.0f}_perc_ci_top',
    }

    #df_results = _bootstrap_quantiles(df_results, conf_interval, **colnames)
    df_results = _bootstrap_ci_from_std(df_results.index, df_results.std(axis=1).values, median, alpha=conf_interval, **colnames)

    # samples = df.quantile(.5, axis=1)
    # x = samples.index.values.astype(int)
    # y = samples.values.astype(float)
    # eval_x = x.copy()
    # median = sm.nonparametric.lowess(exog=x, endog=y, xvals=eval_x, frac=frac)

    df_results['median'] = median

    if clip_to_zero:
        # clip negative values to 0
        df_results[df_results < 0] = 0

    return df_results


def lowess_per_col(df, columns, bootstrap_iters=config['bootstrap_iters'], conf_interval=0.95, lowess_kw=None, clip_to_zero=True):
    """
    Perform Lowess regression and determine a confidence interval by bootstrap resampling
    """

    # add missing days in index
    df = df.astype(pd.Float64Dtype()).resample('D').mean().sort_index()

    df_ret = pd.DataFrame(index=df.index)

    if lowess_kw is None:
        lowess_kw = {}

    print('Smoothing using lowess and generating 95% CI by bootstrap resampling')

    for col in tqdm(columns, unit='column'):
        if 'GM0148' not in col:
            continue

        # y_raw = df[col].replace(0, np.nan).astype(float)
        #
        # # interpolate between datapoints linearly
        # y = y_raw.interpolate('linear', limit=14)  # interpolate max. number of limit days
        #

        idx_start = df[col].first_valid_index()
        idx_end = df[col].last_valid_index()

        df_sel = df[col].loc[idx_start:idx_end].astype(float)
        df_sel = df_sel.interpolate('linear', limit=14)
        #df_sel = df_sel.sort_index()

        # y = y.loc[idx_start:idx_end].dropna()
        # idx_sel = y.index
        # #y_series = y_raw.loc[idx_start:idx_end]
        # y = y.values
        #
        # x = idx_sel.map(lambda x: (x - idx_sel[0]).days)
        #
        # # evaluate over all days between idx_start and idx_end
        # eval_x = x.copy()

        # consider all datapoints at 3 weeks around it
        frac = np.float64(1) / ((idx_end - idx_start) / np.timedelta64(3, 'W'))

        if frac > 1:
            # this means there is < 3W of data
            # we should probably ignore the data if this is the case
            # but for now set frac to 1 (use all samples)
            frac = 1

        local_run_lowess_kw = {**lowess_kw}
        if 'frac' not in local_run_lowess_kw:
            local_run_lowess_kw['frac'] = frac

        # x = df.index.values.astype(int)
        # eval_x = x.copy()
        # y = df[col].astype(float).interpolate('linear', limit=14).values
        # # run lowess smoother
        # smoothed = sm.nonparametric.lowess(exog=x, endog=y, xvals=eval_x, **local_run_lowess_kw)

        smoothed = _lowess_on_df(df_sel, local_run_lowess_kw)
        #smoothed = _lowess_on_df(_quantile_resampling(df_sel, q=.5, iters=1000), local_run_lowess_kw)

        #resample_lambda = lambda df_l: _quantile_resampling(df_l)
        #resample_lambda = lambda df_l: _quantile_resampling(df_l, q=None)
        resample_lambda = _quantile_resampling

        n_jobs = config['n_jobs']
        remainder = bootstrap_iters % n_jobs
        extra_jobs_mod = n_jobs // remainder
        itersize = bootstrap_iters // n_jobs
        iters = [itersize + (x % extra_jobs_mod == 0) for x in range(n_jobs)]

        # Perform bootstrap resampling of the data
        # and  evaluate the smoothing at points
        with tqdm_joblib(tqdm(total=n_jobs, unit=' bootstrap resampling workers finished', leave=False)) as progress_bar:
            #retvals = Parallel(n_jobs=config['n_jobs'])(
            #    delayed(_calc_bootstrap_iter_random_choice)(x, y, eval_x, local_run_lowess_kw) for i in range(bootstrap_iters)
            #)
            retvals = Parallel(n_jobs=n_jobs)(
                delayed(_lowess_worker_with_func_resampler)(iters[i], df_sel, resample_lambda, local_run_lowess_kw) for i in range(n_jobs)
            )

        df_results = _merge_lowess_worker_results(df_sel.index, retvals)

        colnames = {
            'bottom_col': f'{col}_lowess_{conf_interval * 100:0.0f}_perc_ci_bottom',
            'top_col': f'{col}_lowess_{conf_interval * 100:0.0f}_perc_ci_top',
        }

        df_results = _bootstrap_ci_from_std(df_results.index, df_results.std(axis=1).values, smoothed, alpha=conf_interval, **colnames)

        #smoothed = df_results.quantile(.5, axis=1).T
        #df_results = _bootstrap_quantiles(df_results, conf_interval, **colnames)

        df_results[f'{col}_lowess'] = smoothed

        df_ret = df_ret.join(df_results)

    if clip_to_zero:
        # clip negative values to 0
        df_ret[df_ret < 0] = 0

    return df_ret.sort_index()
