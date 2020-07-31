# coding=utf-8
"""compare.py - Script for performance.
"""
import pymysql
import sqlalchemy
import pandas as pd
from pd2ml.utils import timeit
from pd2ml import Loader

con = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='123456', db='learn')
engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@localhost:3306/learn?charset=utf8&local_infile=1')
insert_sql = "INSERT INTO stock (code, open, close, high, low, time) VALUES (%s, %s, %s, %s, %s, %s)"
replace_sql = "REPLACE INTO stock (code, open, close, high, low, time) VALUES (%s, %s, %s, %s, %s, %s)"
df = pd.read_csv('stock.csv', dtype={'code': str})
df.drop_duplicates(subset=['code', 'time'], inplace=True)


@timeit
def insert_multiple_rows(is_replace=False):
    sql = replace_sql if is_replace else insert_sql
    cur = con.cursor()
    cur.executemany(sql, df.values.tolist())
    con.commit()
    con.close()


@timeit
def my_load_to():
    # Another way to write it:
    # df.load_to(stock, 'engine')

    Loader(engine).load_to(df, 'stock')


@timeit
def pd_read_from_table():
    return pd.read_sql_table('stock', engine)


@timeit
def my_load_from():
    # Another way to write it:
    # _ = pd.DataFrame()
    # df = _.load_from('stock', engine)

    return Loader(engine).load_from('stock')


if __name__ == '__main__':
    insert_multiple_rows(is_replace=True)
    my_load_to()
    df1 = pd_read_from_table()
    df2 = my_load_from()
    print(df1 == df2)
    print('finish.')
