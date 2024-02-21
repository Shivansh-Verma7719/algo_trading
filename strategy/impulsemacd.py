# Standard imports
import pandas as pd
import numpy as np
import talib as tl
from os import environ as env


#--------------------------INPUTS--------------------------------
lengthMA: int = 34
lengthSignal: int = 9

#--------------------------FUNCIONS------------------------------
def calc_smma(src: np.ndarray, length: int) -> np.ndarray:
    """
    Calculate Smoothed Moving Average (SMMA) for a given numpy array `src` with a specified `length`.

    :param src: A numpy ndarray of shape (n,) containing the input values of float64 dtype.
    :param length: An integer representing the length of the SMMA window.
    :return: A numpy ndarray of the same shape as `src` containing the SMMA values.
    """
    smma = np.full_like(src, fill_value=np.nan)
    sma = tl.SMA(src, length)

    for i in range(1, len(src)):
        smma[i] = (
            sma[i]
            if np.isnan(smma[i - 1])
            else (smma[i - 1] * (length - 1) + src[i]) / length
        )

    return smma

def calc_zlema(src: np.ndarray, length: int) -> np.ndarray:
    """
    Calculates the zero-lag exponential moving average (ZLEMA) of the given price series.

    :param src: The input price series of float64 dtype to calculate the ZLEMA for.
    :param length: int The number of bars to use for the calculation of the ZLEMA.
    :return: A numpy ndarray of ZLEMA values for the input price series.
    """
    ema1 = tl.EMA(src, length)
    ema2 = tl.EMA(ema1, length)
    d = ema1 - ema2
    return ema1 + d


def macd(data):
    src = (
        data["inth"].to_numpy(dtype=np.double)
        + data["intl"].to_numpy(dtype=np.double)
        + data["intc"].to_numpy(dtype=np.double)
    ) / 3
    hi = calc_smma(data["inth"].to_numpy(dtype=np.double), lengthMA)
    lo = calc_smma(data["intl"].to_numpy(dtype=np.double), lengthMA)
    mi = calc_zlema(src, lengthMA)

    md = np.full_like(mi, fill_value=np.nan)

    conditions = [mi > hi, mi < lo]
    choices = [mi - hi, mi - lo]

    md = np.select(conditions, choices, default=0)

    sb = tl.SMA(md, lengthSignal)
    sh = md - sb

    res = pd.DataFrame(
        {
            "open_time": data["time"],
            "ImpulseMACD": md,
            "ImpulseHisto": sh,
            "ImpulseMACDCDSignal": sb,
        }
    )
    return res


