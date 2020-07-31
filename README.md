<div align="center">
 <div>
   <img src="https://user-images.githubusercontent.com/32212649/88253383-5510de00-cce4-11ea-8773-467c51b6c0bf.png">
  </div>
</div>

## What is it?

**pd2ml** (**p**andas **D**ataFrame **to** **M**ySQL **L**oader) is a Python package that provides an efficient way to upload Pandas `DataFrame` to `MySQL` and download from the database table into a DataFrame. The application of MySQL statements `LOAD DATA LOCAL INFILE` and `SELECT INTO OUTILE` are the most essential reason for such efficient upload and download.It turns out that the advantages of pd2ml will gradually become apparent as the amount of data increases, so it will be a good assistant when dealing with massive amounts of data.

To make a digression, the name pd2ml can be pronounced as pudumal (/'pudumɔ:/), which is a homonym for purdy cat in Chinese, and this is also the inspiration for the logo of this package.

## How to get it

You can install using the pip package manager by running
```sh
pip install pd2ml
```
Note that it is necessary for pd2ml to run in the `Python3` environment.

## Dependencies

- [pandas](https://pandas.pydata.org/)
- [NumPy](https://www.numpy.org)
- [SQLAlchemy](https://www.sqlalchemy.org/)

## Examples

It is veary easy to get started and easy to use.

Firstly, start by reading a csv file into your pandas DataFrame, e.g. by using
```python
import pandas as pd
import sqlalchemy as sa
engine = sa.create_engine('mysql+pymysql://username:pwd@localhost:3306/db?charset=utf8&local_infile=1')
df = pd.read_csv('stock.csv')
```
To upload `df` to database, run
```python
from pd2ml import Loader
Loader(engine).load_to(df, 'stock')
```
To download `df` from databse, run
```python
from pd2ml import Loader
df = Loader(engine).load_from('stock')
print(df)
```
In particular, it supports pandas extension, so you can also use it like this
```python
import pd2ml
# Call it directly just like any other DataFrame's native methods.
# load to db
df.load_to('stock', engine)
# load from db
df = pd.DataFrame()
df = df.load_from('stock', engine)
print(df)
```
What's more, it also works well with multi-processing or multi-threading
```python
# from multiprocessing.pool import ThreadPool as Pool
from multiprocessing.pool import Pool
from utils import split_dataframe, load_to, load_from_
# load to db
with Loader(engine):
    for d in split_dataframe(df, chunk_size):
        pool.apply_async(load_to, (d, 'stock'))
# load from db
with Loader(engine):
    for table in tables:
        result = pool.apply_async(load_from_, (table, get_engine, ))
        result_arr.append(result)
```
For more details about examples, please see [here](https://github.com/TanyeeZhang/pd2ml/tree/master/examples).

## Tips

To ensure that pd2ml works well, here are some tips and suggestions
- It is essential to add parameters `infile_local=1` when connecting to the database
- To make sure pd2ml works, it must be set `secure-file-priv=""` in MySQL configuration file `my.ini` or `my.cnf`
- Meanwhile, to maximize efficiency, the value `innodb_buffer_pool_size` may be adjusted appropriately in configuration file

## Performance

To estimate the performance of pd2ml, I did a preliminary rough test on my laptop.

#### Test Environment

- MySQL 5.7
- Windows 10
- ThinkPad of 4 cores and 8G RAM

#### Test Items

I created a table named stock, this table has 6 fields, of which 2 primary keys, 3 indexes. Then

- Use pd2ml and multi-value insert SQL (`INSERT INTO ... VALUES (), () ...`) to insert 10000/100000/1000000 records 10 times into the empty table respectively, the average elapsed time is shown in the figure

![compare_1](https://user-images.githubusercontent.com/32212649/89024505-ce997380-d357-11ea-99c5-86049a12fe7d.png)

- Use p2dml and multi-value replace SQL (`REPLACE INTO ... VALUES (), () ...`) to insert 10000/100000/1000000 records 10 times into the non-empty table, the average elapsed time is shown in the figure

![compare_2](https://user-images.githubusercontent.com/32212649/89024771-38198200-d358-11ea-948f-5993fc5095e5.png)

- Use pd2ml and pandas native method `read_sql_table` to read 10000/100000/1000000 records 10 times from the table as DATAFRAME, the average elapsed time is shown in the figure

![compare_3](https://user-images.githubusercontent.com/32212649/89024806-48316180-d358-11ea-963d-6c29cefc14a7.png)

#### Conclusion

As the amount of data increases, the speed advantage of pd2ml becomes more and more obvious. The efficiency of writing into the database is at least 20% faster, and reading from the databse is improved three to four times.
