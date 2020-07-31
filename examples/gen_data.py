# coding=utf-8
"""gen_data.py - This script is to generate test data.
"""

import numpy as np
import csv
import time
from random import uniform, random, randint
import sqlalchemy


headers = ['code', 'open', 'close', 'high', 'low', 'time']
rows = []
LINES = 100000
file_name = 'stock.csv'
engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@localhost:3306/learn?charset=utf8&local_infile=1')


def gen_date_time():
    a1 = (1990, 1, 1, 0, 0, 0, 0, 0, 0)
    a2 = (2020, 12, 31, 23, 59, 59, 0, 0, 0)
    start = time.mktime(a1)
    end = time.mktime(a2)
    t = randint(start, end)
    date_tuple = time.localtime(t)
    date = time.strftime("%Y-%m-%d", date_tuple)
    yield date


def gen_stock_data():
    while True:
        code = '000{}'.format(str(randint(100, 999)))
        o = uniform(5, 100)
        r = uniform(0.95, 1.05)
        c = o * r
        h = max(o, c) + random()
        l = min(o, c) - random()
        yield code, np.round([o, c, h, l], decimals=2), next(gen_date_time())


def output_stock_csv():
    for code, prices, t in gen_stock_data():
        if len(rows) >= LINES:
            break
        rows.append([code, prices[0], prices[1], prices[2], prices[3], t])

    with open(file_name, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(headers)
        csv_writer.writerows(rows)


def create_db_table():
    sql = """
    CREATE TABLE IF NOT EXISTS `stock` (
    `code` char(6) NOT NULL,
    `open` double DEFAULT NULL,
    `close` double DEFAULT NULL,
    `high` double DEFAULT NULL,
    `low` double DEFAULT NULL,
    `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`code`,`time`),
    KEY `idx_code` (`code`),
    KEY `idx_time` (`time`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    con = engine.connect()
    con.execute(sql)
    con.close()


if __name__ == '__main__':
    output_stock_csv()
    create_db_table()