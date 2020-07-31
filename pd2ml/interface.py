# coding=utf-8
"""interface.py - Interfaces that subclasses need to implement.
"""

import abc
import pandas


class ILoader(abc.ABC):
    """
    Define interface `ILoader`, which means that all subclasses must implement both `load_to` and `load_from` methods.
    """
    @abc.abstractmethod
    def load_to(self, df: pandas.DataFrame, table_name: str) -> None:
        """Upload/import the DataFrame to the database.

        Args:
            df (pandas.DataFrame): The target dataframe to be uploaded.
            table_name (str): The target table name of database.
        """
        raise NotImplemented

    @abc.abstractmethod
    def load_from(self, table_name: str) -> pandas.DataFrame:
        """Download/export the DataFrame from the database.

        Args:
            table_name (str): The target table name of the database.

        Returns:
            pandas.DataFrame: The dataframe downloaded/exported from the database.
        """
        raise NotImplemented
