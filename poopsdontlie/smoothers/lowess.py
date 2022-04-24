import pandas as pd
import statsmodels.api as sm
import numpy as np
from joblib import Parallel, delayed

from tqdm.auto import tqdm

from poopsdontlie.helpers import config
from poopsdontlie.helpers.joblib import tqdm_joblib


def _calc_bootstrap_iter(x, y, eval_x, lowess_kw):
    sample = np.random.choice(len(x), len(x), replace=True)
    sampled_x = x[sample]
    sampled_y = y[sample]

    return sm.nonparametric.lowess(exog=sampled_x, endog=sampled_y, xvals=eval_x, **lowess_kw)

def sma(df, columns, period_days=7):
    """
    Perform simple moving average filter over columns
    """

    # add missing days in index
    df = df.resample('D').last()

    df_ret = pd.DataFrame(index=df.index)

    for col in tqdm(columns, unit='column'):
        smooth = df[col].replace(0, np.nan).astype(float).rolling(7).mean()
        df_ret = df_ret.join(smooth.rename(f'{col}_sma_{period_days}_days'))

    return df_ret


def lowess(df, columns, bootstrap_iters=2000, conf_interval=0.95, lowess_kw=None, clip_to_zero=True):
    """
    Perform Lowess regression and determine a confidence interval by bootstrap resampling
    """

    # add missing days in index
    df = df.resample('D').last()

    df_ret = pd.DataFrame(index=df.index)

    if lowess_kw is None:
        lowess_kw = {}

    print('Smoothing using lowess and generating 95% CI by bootstrap resampling')

    for col in tqdm(columns, unit='column'):
        y_raw = df[col].replace(0, np.nan).astype(float)

        # interpolate between datapoints linearly
        y = y_raw.interpolate('linear')

        idx_start = y.first_valid_index()
        idx_end = y.last_valid_index()
        y = y.loc[idx_start:idx_end]#.dropna()
        idx_sel = y.index
        y_series = y_raw.loc[idx_start:idx_end]
        y = y.values

        x = idx_sel.map(lambda x: (x - idx_sel[0]).days)

        # evaluate over all days between idx_start and idx_end
        eval_x = x.copy()

        # consider all datapoints at ~1 month around it
        frac = np.float64(1) / ((idx_end - idx_start) / np.timedelta64(3, 'W'))
        if frac > 1:
            # this means there is < 1 month of data
            frac = 1

        local_run_lowess_kw = {**lowess_kw}
        if 'frac' not in local_run_lowess_kw:
            local_run_lowess_kw['frac'] = frac

        # run lowess smoother
        smoothed = sm.nonparametric.lowess(exog=x, endog=y, xvals=eval_x, **local_run_lowess_kw)

        # Perform bootstrap resampling of the data
        # and  evaluate the smoothing at points
        with tqdm_joblib(tqdm(total=bootstrap_iters, unit=' bootstrap resampling iterations', leave=False)) as progress_bar:
            retvals = Parallel(n_jobs=config['n_jobs'])(
                delayed(_calc_bootstrap_iter)(x, y, eval_x, local_run_lowess_kw) for i in range(bootstrap_iters)
            )

        smoothed_values = np.empty((bootstrap_iters, len(eval_x)))
        for i in range(bootstrap_iters):
            smoothed_values[i] = retvals[i]

        # # Get the confidence interval
        # sorted_values = np.sort(smoothed_values, axis=0)
        # # some bootstrap iters return NaN, drop those
        # sorted_values = sorted_values[~np.isnan(sorted_values)]
        # count = len(sorted_values)
        # bound = int(count * (1 - conf_interval) / 2)
        # bottom = sorted_values[bound - 1]
        # top = sorted_values[-bound]
        #
        # # DEBUG DEBUG DEBUG
        # print(top)
        # with open('/tmp/test.pickle', 'wb') as fh:
        #     import pickle
        #     pickle.dump(top, fh)



        quant = np.nanquantile(smoothed_values, [.025, .5, .975], axis=0)

        df_result = pd.DataFrame(index=[idx_start, idx_end])
        df_result = df_result.resample('D').last()

        #df_result[f'{col}_{int(conf_interval*100)}%_ci_bottom'] = bottom
        df_result[f'{col}_{int(conf_interval * 100)}%_ci_bottom'] = quant[0]

        #df_result[f'{col}_smoothed'] = smoothed
        df_result = df_result.join(y_series.rename(f'{col}_raw'))

        df_result[f'{col}_median'] = quant[1]

        #df_result[f'{col}_{int(conf_interval*100)}%_ci_top'] = top
        df_result[f'{col}_{int(conf_interval * 100)}%_ci_top'] = quant[2]

        df_ret = df_ret.join(df_result)

        if clip_to_zero:
            # clip negative values to 0
            df_ret[df_ret < 0] = 0

    return df_ret
