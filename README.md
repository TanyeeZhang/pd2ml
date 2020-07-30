<div align="center">
 <div>
   <img src="https://user-images.githubusercontent.com/32212649/88253383-5510de00-cce4-11ea-8773-467c51b6c0bf.png">
  </div>
</div>

## What is it?

**pd2ml** (**P**andas **D**ataFrame **to** **M**ySQL **L**oader) is a Python package that provides an efficient way to upload Pandas `DataFrame` to `MySQL` and download from the database table into a DataFrame. The application of MySQL statements `LOAD DATA LOCAL INFILE` and `SELECT INTO OUTILE` are the most essential reason for such efficient upload and download.It turns out that the advantages of pd2ml will gradually become apparent as the amount of data increases, so it will be a good assistant when dealing with massive amounts of data.

To make a digression, the name pd2ml can be pronounced as pudumal (/'pudumɔ:/), which is a homonym for purdy cat in Chinese, and this is also the inspiration for the logo of this package.

## Where to get it

You can install using the pip package manager by running
```sh
pip install pd2ml
```

## Examples

Start by reading a csv file into your pandas DataFrame, e.g. by using
```python
import pandas as pd
import sqlalchemy
engine = sqlalchemy.create_engine('mysql+pymysql://username:password@localhost:3306/db?charset=utf8&local_infile=1')
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
It still works well with multi-processing or multi-threading
```python
# from multiprocessing.pool import ThreadPool as Pool
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
from pd2ml.utils import split_dataframe, load_to, load_from_
# load to db
pool = Pool(processes=cpu_count())
with Loader(engine):
    for d in split_dataframe(df, chunk_size):
        pool.apply_async(load_to, (d, 'stock'))
    pool.close()
    pool.join()
# load from db
tables = ['stock', 'stock_1', 'stock_2', 'stock_3']
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
```
In particular, it supports pandas extension, so you can also use it like this
```python
from pd2ml import loader_ext
# Call it directly just like any other DataFrame's native methods.
# load to db
df.load_to('stock', engine)
# load from db
df = pd.DataFrame()
df = df.load_from('stock', engine)
print(df)
```
