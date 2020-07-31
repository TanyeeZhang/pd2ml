# coding=utf-8
"""

"""
import sqlalchemy
import pandas as pd
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
from pd2ml.utils import split_dataframe, load_to, load_from_
from pd2ml.loader import Loader

engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@localhost:3306/learn?charset=utf8&local_infile=1')
df = pd.read_csv('stock.csv', dtype={'code': str})


def my_load_to(df, table_name):
    """Perform some intermediate steps."""

    """Add your code here"""

    return df, table_name


def get_engine():
    """You need to return `engine` as a function."""

    return sqlalchemy.create_engine('mysql+pymysql://root:123456@localhost:3306/learn?charset=utf8&local_infile=1')


if __name__ == '__main__':
    engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@localhost:3306/learn?charset=utf8&local_infile=1')

    # from multiprocessing.pool import ThreadPool as Pool
    # Use in the same way for multi-threading.

    pool = Pool(processes=cpu_count())
    with Loader(engine):
        for d in split_dataframe(df, 25000):
            pool.apply_async(load_to, (d, 'stock'))
            # pool.apply_async(load_to_, (my_load_to, d, 'stock'))
        pool.close()
        pool.join()

    tables = ['stock'] * 4
    result_arr = []
    pool = Pool(processes=cpu_count())
    with Loader(engine):
        for table in tables:
            result = pool.apply_async(load_from_, (table, get_engine, ))
            result_arr.append(result)
        pool.close()
        pool.join()
    for r in result_arr:
        print(r.get())

    print('finish.')