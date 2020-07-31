# coding=utf-8
"""manager.py - Module that manages files during the load process.
"""

import os
from typing import List
from pathlib import Path
import shutil
import threading
import sys
import platform
import csv
import pandas
import pickle

__all__ = ['UpLoadManger', 'DownLoadManager']


class FileManager:
    """This class is to manage files during upload and download."""

    def get_current_argv(self) -> str:
        """Get the path to the currently running script."""

        return sys.argv[0]

    def get_parent_dir(self) -> str:
        """Get the directory path of the currently running script."""

        return os.path.dirname(self.get_current_argv())

    def get_current_name(self):
        """Get the file name of the currently running script."""

        return os.path.splitext(os.path.basename(self.get_current_argv()))[0]

    def get_dir(self, prefix: str) -> str:
        """Get the temporary directory path."""

        return self.format_path(os.path.join(self.get_parent_dir(), prefix + self.get_current_name()))

    def get_child_dir(self, work_dir: str) -> str:
        """Get the folder path for each temporary file.

            Notes:
                To ensure that writing to a file is uninterrupted in the case of multi-threading or multi-processing,
                it's distinguish by the thread identifier and the process id.
        """

        return self.format_path(os.path.join(work_dir, str(os.getpid()) + '_' +
                                             str(threading.current_thread().ident)))

    def get_table_name(self, path) -> str:
        """Get the table name based on the path.

        Examples:
            >>> FileManager().get_table_name('/tmp/loader/stock.csv')
            'stock'
        """

        return os.path.splitext(os.path.basename(path))[0]

    def get_terminate(self) -> str:
        """Get line breaks depending on the system."""

        return '\r\n' if platform.system() == 'Windows' else '\n'

    def mkdirs(self, dirs: List[str]) -> None:
        """Create directories."""

        for d in dirs:
            if not os.path.exists(d):
                try:
                    os.mkdir(d)
                except:
                    pass

    def clear(self, dirs: List[str]) -> None:
        """Clear directories."""

        for d in dirs:
            if os.path.exists(d):
                try:
                    shutil.rmtree(d)
                except Exception as e:
                    print(e)

    def format_path(self, path: str) -> str:
        """Format the paths uniformly to the Linux style.

        Examples:
            >>> FileManager().format_path('D:/tmp/loader\\stock.csv')
            'D:/tmp/loader/stock.csv'
        """

        return Path(path).as_posix()

    def get_no_confict_path(self, path: str) -> str:
        """If a file already exists, add '.1' to the end of the file name
        until there is no conflict."""

        while os.path.exists(path):
            path = path + '.1'
        return path


class UpLoadManger(FileManager):
    """The Class that specifically deal with details of `UpLoader`."""

    def write_to_csv(self, name: str, df: pandas.DataFrame) -> None:
        """Write the dataframe into a csv file.

        Args:
            name (str): Name of csv file.
            df (pandas.DataFrame): The dataframe to be written.
        """

        with open(name, 'a+', newline='', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(df.values.tolist())

    def pickle_to(self, path: str, s: str) -> None:
        """Write into disk by pickle."""

        with open(path, 'wb') as out:
            pickle.dump(s, out)

    def pickle_from(self, path: str) -> str:
        """Read from disk by pickle."""

        with open(path, 'rb') as inputs:
            obj = pickle.load(inputs)
        return obj


class DownLoadManager(FileManager):
    """The Class that specifically deal with details of `DownLoader`."""

    def read_from_csv(self, path: str, columns: List[str]) -> pandas.DataFrame:
        """Read from the file and create the dataframe.

        Args:
            path (str): Path to file.
            columns (list): List that contains names of columns.

        Returns:
            pandas.DataFrame: The created dataframe from a csv file.
        """

        rows = []
        with open(path, encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                rows.append(row)
        return pandas.DataFrame(rows, columns=columns)
