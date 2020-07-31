# coding=utf-8
import pandas as pd
from .loader import Loader


@pd.api.extensions.register_dataframe_accessor('load_to')
class UpLoaderExt(Loader):
    def __init__(self, obj):
        super().__init__()
        self.obj = obj

    def __call__(self, *args, **kwargs):
        table_name, engine = args
        return Loader(engine).load_to(self.obj, table_name)


@pd.api.extensions.register_dataframe_accessor('load_from')
class DownLoaderExt(Loader):
    def __init__(self, obj):
        super().__init__()
        self.obj = obj

    def __call__(self, *args, **kwargs):
        table_name, engine = args
        return Loader(engine).load_from(table_name)