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
