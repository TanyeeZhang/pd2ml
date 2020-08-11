# coding=utf-8
"""loader.py - Core modules for uploading to database and downloading from database of dataframes.
"""

import os
import sqlalchemy
import numpy as np
import pandas
from sqlalchemy import text
from retry import retry
from .common import UPLOAD_DIR_PREFIX, UPLOAD_MODE, LOAD_FILE_SUFFIX, COLUMNS_FILENAME, DOWNLOAD_DIR_PREFIX, \
    TYPE_MAPPING, UPLOAD_COMMAND, DOWNLOAD_COMMAND, COLUMNS_COMMAND
from .manager import UpLoadManger, DownLoadManager
from .interface import ILoader
from typing import List, Tuple, Union


class UpLoader:
    """UpLoader is the concrete implementation class that implements the `load_to` function."""

    PREFIX = UPLOAD_DIR_PREFIX  # Temporary folder name prefix
    WRITE_MODE = UPLOAD_MODE  # 'REPLACE' or 'IGNORE' on duplicate rows
    FILE_SUFFIX = LOAD_FILE_SUFFIX  # File name suffix, e.g. '.txt' or '.csv'
    COLUMNS_FN = COLUMNS_FILENAME  # Name of file that save dataframe columns

    def __init__(self, engine: sqlalchemy.engine = None):
        """
        Args:
            engine (sqlalchemy.base.Engine): engine instance by `create_engine` function.
        Notes:
            The string form of the URL in ` create_engine` is
            dialect[+driver]://user:password@host/dbname[?key=value..]

            DON'T forget to put 'local_infile=1' into the URL.

            e.g.
            sqlalchemy.create_engine('mysql+pymysql://root:123456@localhost:3306/learn?charset=utf8&local_infile=1')
        """

        self.engine = engine
        self.manager = UpLoadManger()
        self.dir = self.manager.get_dir(UpLoader.PREFIX)
        self.child_dir = self.manager.get_child_dir(self.dir)
        self.str_columns = ''
        self.terminate = self.manager.get_terminate()
        self.__execute_list = []
        self.__str_col_path = self.manager.format_path(os.path.join(self.dir, UpLoader.COLUMNS_FN))

    def __make_tmp_dir(self):
        """Creating working directories."""

        self.manager.mkdirs([self.dir, self.child_dir])

    def __make_tmp_table_path(self, name: str) -> str:
        """Set and get the path to the temporary csv table.

        Args:
            name (str):  table_name.

        Returns:
            str: the path to the temporary csv table.
        """

        self.__make_tmp_dir()

        return os.path.join(self.child_dir, name + UpLoader.FILE_SUFFIX)

    def __create_tmp_table(self, obj: pandas.DataFrame) -> None:
        """Write the dataframe into the temporary csv table.

        Args:
            obj (pandas.DataFrame): The dataframe to be uploaded.

        Raises:
            AttributeError: If the `obj` dose not has '_table_name_' attribute.
        """

        if hasattr(obj, '_table_name_'):
            name = getattr(obj, '_table_name_')
            tmp_table_name = self.__make_tmp_table_path(name)
            self.manager.write_to_csv(tmp_table_name, obj)
        else:
            raise AttributeError('The returned value should have an attribute value as _table_name_.')

    def make_tmp_table(self, obj: Union[pandas.Series, pandas.DataFrame]) -> None:
        """Create the temporary csv table and save the columns information.

        Args:
            obj (pandas.Series or pandas.DataFrame): The target dataframe or series.

        Raises:
            ValueError: If `obj` is not DataFrame or Series.
        """

        if isinstance(obj, pandas.DataFrame):
            pass
        elif isinstance(obj, pandas.Series):
            _name = getattr(obj, '_table_name_')
            obj = obj.to_frame()
            print(obj)
            print(obj.info())
            setattr(obj, '_table_name_', _name)
        else:
            raise ValueError('The returned value should be dataFrame or series.')

        self.__create_tmp_table(obj)

        self.__save_columns(obj)

    def __save_columns(self, obj: pandas.DataFrame) -> None:
        """Save columns into disk by pickling.
        The purpose of saving it is to use it when uploading.

        Args:
            obj (pandas.DataFrame): The target dataframe.

        Notes:
            The columns of dataframe to be uploaded must be a SUBSET (contains its own)
            of the columns of target table in the database.
        """

        __path = self.manager.format_path(os.path.join(self.dir, 'str_columns'))

        if not os.path.exists(__path):
            str_col = obj.columns.tolist()
            str_columns = str(str_col).replace('[', '(').replace(']', ')').replace("'", '')
            self.manager.pickle_to(__path, str_columns)

    def __make_null_str(self, str_columns: str) -> Tuple:
        """Set a NULL value into the database, when the value in dataframe is NaN.

        Args:
            str_columns: The names of columns in string.

        Returns:
            tuple: A tuple of two strings such as `(database columns variables, NULLIF expressions)`.
            e.g. ("(@vcode, @vtime)", "code = NULLIF(@vcode, 'nan'), time = NULLIF(@vtime, 'nan')")
        """

        cols = str_columns.replace('(', '').replace(')', '').split(',')
        modified_str = str(list(map(lambda x: '@v{}'.format(str(x).strip()), cols))).replace('[', '('). \
            replace(']', ')').replace("'", '')
        is_null_str = str(list(map(lambda x: "{0} = NULLIF(@v{0}, 'nan')".format(str(x).strip()), cols))). \
            replace('[', '').replace(']', '').replace('"', '')

        return modified_str, is_null_str

    @retry(tries=5, delay=3)
    def __exec_command(self, file_path: str, table_name: str, str_columns: str) -> None:
        """Execute upload command.

        Args:
            file_path (str): Path of the csv table file.
            table_name (str): Name of table in database.
            str_columns (str): Information of columns that previously saved.

        Notes:
            If it fails, try again five times, each interval of three seconds.
        """

        try:
            self.conn = self.engine.connect()
            modified_str, isnull_str = self.__make_null_str(str_columns)
            load_sql = text(
                UPLOAD_COMMAND.format(file_path, UpLoader.WRITE_MODE, table_name, self.terminate, modified_str, isnull_str))
            self.conn.execute(load_sql)
        except Exception as e:
            print(e)
            raise e

    def execute(self):
        """Execute upload command and control the whole process.

        The main steps are as follows:
        1. Iterate over all the files in the folder except `COLUMNS_FILENAME` and append them to the list;
        2. Upload files from the list via the upload command `LOAD DATA LOCAL INFILE` in MySQL;
        3. Delete the folder after uploading.
        """

        if not os.path.exists(self.__str_col_path):
            return

        self.str_columns = self.manager.pickle_from(self.__str_col_path)

        for dir_path, dir_names, file_names in os.walk(self.dir):
            for file_name in file_names:
                if file_name != COLUMNS_FILENAME:
                    self.__execute_list.append(self.manager.format_path(os.path.join(dir_path, file_name)))

        for file_path in self.__execute_list:
            try:
                self.__exec_command(file_path, self.manager.get_table_name(file_path), self.str_columns)
            except Exception as e:
                raise e
            finally:
                self.conn.close()

        self.__execute_list.clear()
        self.clear()

    def load_to(self, df: pandas.DataFrame, table_name: str) -> None:
        """Provide the public external method for uploading.

        Args:
            df (pandas.DataFrame): The target dataframe.
            table_name (str): Name of table in database.
        """

        self.clear()
        self.set_attr_tn(df, table_name)
        self.make_tmp_table(df)
        self.execute()

    def batch_load_to(self, df: pandas.DataFrame, table_name: str) -> None:
        """Provide the public external method for batch uploading.
        Mainly used for multi-threading and multi-processing within `with` statement.

        Args:
            df (pandas.DataFrame): The target dataframe.
            table_name (str): Name of table in database.
        """

        self.set_attr_tn(df, table_name)
        self.make_tmp_table(df)

    def set_attr_tn(self, df: pandas.DataFrame, table_name: str) -> None:
        """Set it an attribute `_table_name_` to get the database table name.

        Args:
            df (pandas.DataFrame): The target dataframe.
            table_name (str): Name of table in database.
        """

        setattr(df, '_table_name_', table_name)

    def __enter__(self):
        """Prepare something when entering into `with Loader(engine):` statement.
        Here need to clear files that were not cleared last time.
        """

        self.clear()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Do something when leaving away `with Loader(engine):` statement.
        Here call `execute()` method to upload the dataframe.
        """

        self.execute()

    def clear(self):
        """Clear temporary folders."""

        self.manager.clear([self.dir])


class DownLoader:
    """DownLoader is the concrete implementation class that implements the `load_from` function."""

    PREFIX = DOWNLOAD_DIR_PREFIX  # Temporary folder name prefix
    FILE_SUFFIX = LOAD_FILE_SUFFIX  # File name suffix, e.g. '.txt' or '.csv'

    def __init__(self, engine: sqlalchemy.engine = None):
        """
        Args:
            engine (sqlalchemy.base.Engine): engine instance by `create_engine` function
        Notes:
            To ensure that this class can normally works, we need to ensure that
            the database is configured by `secure-file-priv=""`
            For setting this parameter, see the following link:
            https://stackoverflow.com/questions/32737478/how-should-i-tackle-secure-file-priv-in-mysql
            https://www.digitalocean.com/community/questions/mysql-can-t-use-load-data-infile-secure-file-priv-option-is-preventing-execution
        """

        self.engine = engine
        self.manager = DownLoadManager()
        self.dir = self.manager.get_dir(DownLoader.PREFIX)
        self.child_dir = self.manager.get_child_dir(self.dir)
        self.terminate = self.manager.get_terminate()
        self.conn = None
        self.file_path = ''

    def __get_columns(self, table_name: str) -> List[Tuple]:
        """Get columns of table in database.

        Args:
            table_name (str): Name of table in database.

        Returns:
            list: A list of tuples, the both of element of which represents
            the column name and the second the data type respectively.
            e.g. [('column1', 'char(6)'), ('column2', 'int'), ...]
        """

        # Read from database
        self.conn = self.engine.connect()
        try:
            column_schema = list(self.conn.execute(text(COLUMNS_COMMAND.format(table_name))))
        except Exception as e:
            self.clear()
            raise e

        columns = []
        type_value = 'object'  # default

        for column in column_schema:
            t = column[1].split('(')[0]  # e.g. `char(6)` -> `char`
            for type_key in TYPE_MAPPING:
                types = TYPE_MAPPING[type_key]
                if t in types:
                    type_value = type_key
                    break
            columns.append((column[0], type_value))

        return columns

    def __exec_command(self, table_name: str, if_null_str: str) -> None:
        """Execute download command via `SELECT * INTO OUTFILE`

        Args:
            table_name (str): Name of table in database.
        """

        sql = text(DOWNLOAD_COMMAND.format(if_null_str, self.file_path, self.terminate, table_name))
        try:
            self.conn.execute(sql)
        except Exception as e:
            self.clear()
            raise e
        self.conn.close()

    def __convert_type(self, df: pandas.DataFrame, columns: List[Tuple]) -> pandas.DataFrame:
        """Convert the table to the same type as Pandas based on the field type of the database.

        Args:
            df (pandas.DataFrame): The dataframe read from the csv file.
            columns (List[Tuple]): A list of tuples.

        Returns:
            pandas.DataFrame: The converted dataframe.
        """
        # Convert a null value to NaN.
        df.replace('', np.nan, inplace=True)

        for column, t in columns:
            df[[column]] = df[[column]].astype(t)

        return df

    def load_from(self, table_name: str, is_batch: bool = False) -> pandas.DataFrame:
        """Execute upload command and control the whole process.

        The main steps are as follows:
        1. Get all column information of database;
        2. Execute download command;
        3. Read the dataframe from the downloaded file and convert it.

        Args:
            table_name (str): Name of table in database.
            is_batch (bool): If True, use with `with Loader(engine)` statement to batch.
            Otherwise, it represents a single execution.

        Returns:
            pandas.DataFrame: The final target dataframe.
        """

        self.manager.mkdirs([self.dir, self.child_dir])
        file_path = self.manager.format_path(os.path.join(self.child_dir, table_name + DownLoader.FILE_SUFFIX))
        self.file_path = self.manager.get_no_confict_path(file_path)
        columns = self.__get_columns(table_name)
        cols = [c[0] for c in columns]

        # NOTE
        # Output a null value as a null character. `null` -> `''`
        # By default the null value will be written as '\N' in the csv file.

        if_null_str = ','.join(map(lambda x: "IFNULL({0}, '') as {0}".format(x), cols))
        self.__exec_command(table_name, if_null_str)
        _df = self.manager.read_from_csv(self.file_path, cols)

        if not is_batch:
            self.clear()

        return self.__convert_type(_df, columns)

    def clear(self):
        """Clear temporary folders."""

        self.manager.clear([self.dir])

    def __enter__(self):
        """Prepare something when entering into `with Loader(engine):` statement.
        Here need to clear files that were not cleared last time.
        """

        self.clear()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Do something when leaving away `with Loader(engine):` statement.
        Here Clear temporary folders.
        """

        self.clear()


class Loader(ILoader):
    """The implementation class of the interface `ILoader`.

    Notes:
        To ensure efficiency of class `Loader`, you may need to adjust the parameters
        `innodb_buffer_pool_size` of the database configuration.
        See links about it:
        https://dev.mysql.com/doc/refman/5.7/en/innodb-buffer-pool-resize.html
    """

    def __init__(self, engine: sqlalchemy.engine = None) -> None:
        """It's made up of two instances of class `UpLoader` and `DownLoader`."""

        self.__up_loader = UpLoader(engine)
        self.__down_loader = DownLoader(engine)

    def load_to(self, df: pandas.DataFrame, table_name: str) -> None:
        """The single upload to database."""

        self.__up_loader.load_to(df, table_name)

    def load_from(self, table_name: str) -> pandas.DataFrame:
        """The single download from database."""

        return self.__down_loader.load_from(table_name)

    def batch_load_to(self, df, table_name):
        """Batch uploading is to write the large dataframe into small dataframes
        into local files and then upload them together."""

        self.__up_loader.set_attr_tn(df, table_name)
        self.__up_loader.make_tmp_table(df)

    def batch_load_from(self, table_name, is_batch=True):
        """Batch downloading is achieved by setting parameters `is_batch`.

        Notes:
            The difference between a batch download and a single download is that
            the temporary folder is deleted together when leaving the `with` statement."""

        return self.__down_loader.load_from(table_name, is_batch)

    def __enter__(self):
        """Call `__enter__` method of `UpLoader` and `DownLoader` respectively."""

        self.__up_loader.__enter__()
        self.__down_loader.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Call `__exit__` method of `UpLoader` and `DownLoader` respectively."""

        self.__up_loader.__exit__(exc_type, exc_val, exc_tb)
        self.__down_loader.__exit__(exc_type, exc_val, exc_tb)