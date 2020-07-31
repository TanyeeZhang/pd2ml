# coding=utf-8
import sqlalchemy
import pandas as pd
import numpy as np
from pandas import DataFrame
from pd2ml import Loader
from pd2ml.utils import timeit

engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@localhost:3306/learn?charset=utf8&local_infile=1')
df = pd.read_csv('stock.csv', dtype={'code': str})


@timeit
def my_load_to():
    Loader(engine).load_to(df, 'stock')


@timeit
def my_load_from():
    return Loader(engine).load_from('stock')


if __name__ == '__main__':
    Loader(engine).load_to(df, 'stock')

    df1 = Loader(engine).load_from('stock')
    print(df1)

    # import pd2ml
    df2 = df[:100]
    df2.load_to('stock', engine)

    df3 = DataFrame()
    df3 = df3.load_from('stock', engine)
    print(df3)

    with Loader(engine) as loader:
        df = df[['code', 'open', 'time']]
        df['open'] = np.nan
        loader.load_to(df, 'stock')
        df4 = loader.load_from('stock')
        print(df4)

    print('finish.')