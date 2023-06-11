"""Common tools for handling dates."""
import calendar
from datetime import date, datetime
from typing import Union

import numpy as np
import pandas as pd


def to_unix(dt: Union[datetime, datetime]) -> int:
    """
    Convert datetime.date or datetime.datetime to unix timestamp microseconds.

    Note: Spark considers that time is stored in parquet as UTC format, whilst pyarrow does not care.
    This can create discrepancies. This is why it is highly recommended to store timestamps as unix timestamps instead
    of datetime objects.

    Args:
        dt: datetime object

    Returns:
        unix timestamp
    """
    if isinstance(dt, datetime):
        return int(calendar.timegm(dt.timetuple()) * 1e6 + dt.microsecond)
    elif isinstance(dt, date):
        return int(calendar.timegm(dt.timetuple()) * 1e6)
    else:
        raise NotImplementedError


def datetime_series_to_unix(s: pd.Series) -> pd.Series:
    """
    Convert a series of datetime.date or datetime.datetime to unix timestamp microseconds.

    Args:
        s: series of datetime.date or datetime.datetime

    Returns:
        pd.Series of unix timestamps
    """
    return pd.to_datetime(s).astype(np.int64) * 0.001  # type: ignore


def unix_series_to_datetime(s: pd.Series) -> pd.Series:
    """
    Convert a series of unix timestamp microseconds to datetime.

    Args:
        s: series of unix timestamp

    Returns:
        series of datetime.datetime
    """
    return pd.to_datetime(s * 1000).dt.to_pydatetime()  # type: ignore


def unix_now() -> int:
    """
    Return now as unix timestamp microseconds.

    Note: Spark considers that time is stored in parquet as UTC format, whilst pyarrow does not care.
    This can create discrepancies. This is why it is highly recommended to store timestamps as unix timestamps instead
    of datetime objects.

    Returns:
        now as unix timestamp
    """
    return to_unix(datetime.now())
