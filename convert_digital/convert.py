from efinance.common import get_realtime_quotes_by_fs
import re
import pandas as pd
from typing import Callable, Dict, Union, List, TypeVar
from functools import wraps
F = TypeVar('F')


def to_numeric(func: F) -> F:
    """
    将 DataFrame 或者 Series 尽可能地转为数字的装饰器

    Parameters
    ----------
    func : Callable
        返回结果为 DataFrame 或者 Series 的函数

    Returns
    -------
    Union[DataFrame, Series]

    """

    ignore = ['股票代码', '基金代码', '代码', '市场类型', '市场编号', '债券代码', '行情ID', '正股代码']

    @wraps(func)
    def run(*args, **kwargs):
        values = func(*args, **kwargs)
        if isinstance(values, pd.DataFrame):
            for column in values.columns:
                if column not in ignore:

                    values[column] = values[column].apply(convert)
        elif isinstance(values, pd.Series):
            for index in values.index:
                if index not in ignore:

                    values[index] = convert(values[index])
        return values

    def convert(o: Union[str, int, float]) -> Union[str, float, int]:
        if o=='-':
            return None
        if not re.findall('\d', str(o)):
            return o
        try:
            if str(o).isalnum():
                o = int(o)
            else:
                o = float(o)
        except:
            pass
        return o
    return run


@to_numeric
def get_inno_company(fs:str):
    df=get_realtime_quotes_by_fs(fs)
    df.rename(columns={'代码': '股票代码',
                       '名称': '股票名称'
                       }, inplace=True)
    return df
if __name__ == '__main__':

    df=get_inno_company("b:BK1005")
    df.to_parquet("inno.parquet")