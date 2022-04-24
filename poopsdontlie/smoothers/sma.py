import numpy as np
import pandas as pd

from tqdm.auto import tqdm


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
