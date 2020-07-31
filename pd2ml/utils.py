# coding=utf-8
"""utils.py - Tools for methods.
"""
import math
import time
from pd2ml.loader import Loader
from functools import wraps


def split_dataframe(df, chunk_size=10000):
    """Split a dataframe into small, fixed-size fragments.

    Args:
        df (pandas.DataFrame): The dataframe to be split.
        chunk_size: The size of a split block.

    Yields:
        pandas.DataFrame: The split fragment of the original dataframe.
    """

    num_chunks = math.ceil(len(df) / float(chunk_size))
    for i in range(num_chunks):
        yield df[i * chunk_size:(i + 1) * chunk_size]


def timeit(func):
    """Calculate the cost time of the function.

    Args:
        func (function): The function to be evaluated.

    Returns:
        int: The cost time of running or execution.
    """

    @wraps(func)
    def _time_it(*args, **kwargs):
        start = int(round(time.time() * 1000))
        try:
            return func(*args, **kwargs)
        finally:
            cost = int(round(time.time() * 1000)) - start
            cost = cost if cost > 0 else 0
            print("Total execution time of function '{}': {} ms".format(str(func.__name__),
                                                                        str(cost)))

    return _time_it


def load_to(df, table_name):
    """Call the function during uploading by multi-threading or multi-processing."""

    Loader().batch_load_to(df, table_name)


def load_to_(func, *args):
    """Call the function during uploading by multi-threading or multi-processing.

    Notes:
        The difference with `load_to` is that this allows you to dynamically
        specify dataframes corresponding to table names in custom functions.
    """

    df, table_name = func(*args)
    load_to(df, table_name)


def load_from_(table_name, func, *args, **kwargs):
    """Call the function during downloading by multi-threading or multi-processing.

    Notes:
        It needs a function that returns `engine`.
    """

    engine = func(*args, **kwargs)
    return Loader(engine).batch_load_from(table_name)
