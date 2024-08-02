import efinance as ef
from datetime import datetime, timedelta
import pandas as pd
from efinance.common import get_realtime_quotes_by_fs
from convert_digital.convert import to_numeric
from .duckdb_handler import DuckDBHandler
import os
import multitasking
from retry import retry
from tqdm import tqdm
from colorama import init, Fore, Style
from typing import Dict, List, Union
"""
    klt : int, optional
        行情之间的时间间隔，默认为 ``101`` ，可选示例如下

        - ``1`` : 分钟
        - ``5`` : 5 分钟
        - ``15`` : 15 分钟
        - ``30`` : 30 分钟
        - ``60`` : 60 分钟
        - ``101`` : 日
        - ``102`` : 周
        - ``103`` : 月
"""
class StockAnalyzer:
    def __init__(self):
        self.extra_fields = {
            'extra_fields': {
                "f37": "Roe",
                "f57": "负债率",
                "f115": "滚动市盈率",
                "f9": "动态市盈率",
                "f100": "行业板块",
            }
        }

    def get_formatted_date(self,days_to_subtract):
        """
        获取当前日期减去指定天数后的日期，并以"YYYYMMDD"格式的字符串返回。

        参数:
        - days_to_subtract: 要减去的天数（整数）

        返回:
        - 减去指定天数后的日期，格式为"YYYYMMDD"的字符串
        """
        # 获取当前日期和时间
        now = datetime.now()

        # 创建一个timedelta对象，表示要减去的天数
        delta = timedelta(days=days_to_subtract)

        # 从当前日期减去指定的天数
        new_date = now - delta

        # 将新的日期格式化为指定的格式
        date_str = new_date.strftime("%Y%m%d")

        return date_str
    @to_numeric
    def get_realtime_stock_info(self):
        series = ef.stock.get_realtime_quotes(None, **self.extra_fields)
        series['涨跌幅'] = pd.to_numeric(series['涨跌幅'], errors="coerce")
        series.sort_values(by=["涨跌幅"], ascending=False, inplace=True)
        series['总市值'] = pd.to_numeric(series['总市值'], errors="coerce")
        series['动态市盈率'] = pd.to_numeric(series['动态市盈率'], errors="coerce")
        series['最低'] = pd.to_numeric(series['最低'], errors="coerce")
        series['最高'] = pd.to_numeric(series['最高'], errors="coerce")
        series['最新价'] = pd.to_numeric(series['最新价'], errors="coerce")
        series['Roe'] = pd.to_numeric(series['Roe'], errors="coerce")
        series['负债率'] = pd.to_numeric(series['负债率'], errors="coerce")
        series['滚动市盈率'] = pd.to_numeric(series['滚动市盈率'], errors="coerce")
        series['换手率'] = pd.to_numeric(series['换手率'], errors="coerce")
        # series["多空对比"] = (series["最新价"] - series["最低"]) / (series["最高"] - series["最低"])
        df=series.reset_index(drop=True)
        return df

    @to_numeric
    def filter_realtime_stock_info(self, df:pd.DataFrame):
        # & (df['动态市盈率'] < df["滚动市盈率"])
        df = df.loc[(df['流通市值'] < 20000000000) & (df['动态市盈率'] > 0) & (df['负债率'] < 60)
                         & (df['换手率'] > 5) & (df["滚动市盈率"] > 0)]
        df2=df.reset_index(drop=True)
        return(df2)

    def get_daily_billboard(self):
        series = ef.stock.get_daily_billboard(str((datetime.datetime.now() - timedelta(days=15)).date()),
                                              str(datetime.datetime.now().date()))
        return series

    @to_numeric
    def get_quote_history(self, df2,days_to_subtract,ktl):
        df2_data = df2['股票代码'].tolist()
        concatenated_df = ef.stock.get_quote_history(df2_data, beg=self.get_formatted_date(days_to_subtract) ,end='20500101', klt=ktl, return_df=True)
        concatenated_df['最低'] = pd.to_numeric(concatenated_df['最低'], errors="coerce")
        concatenated_df['最高'] = pd.to_numeric(concatenated_df['最高'], errors="coerce")
        concatenated_df['收盘'] = pd.to_numeric(concatenated_df['收盘'], errors="coerce")
        concatenated_df['日期'] = pd.to_datetime(concatenated_df['日期'], errors="coerce")
        # concatenated_df["多空对比"] = (concatenated_df["收盘"] - concatenated_df["最低"]) / (
        #             # concatenated_df["最高"] - concatenated_df["最低"])
        return concatenated_df

    @to_numeric
    def get_inno_company(self, fs: str):
        df = get_realtime_quotes_by_fs(fs)
        df.rename(columns={'代码': '股票代码', '名称': '股票名称'}, inplace=True)
        return df

    @to_numeric
    def get_stock_quote(self,code:str):
        stock_info = ef.stock.get_quote_snapshot(code)
        stock_df = stock_info.to_frame().transpose()
        stock_df["时间"]=pd.to_datetime(datetime.now().strftime("%Y-%m-%d ")+stock_df["时间"])
        return stock_df
    @to_numeric
    def get_deal_detail(self,code:str):
        stock_df = ef.stock.get_deal_detail(code)
        stock_df["时间"]=pd.to_datetime(datetime.now().strftime("%Y-%m-%d ")+stock_df["时间"])
        return stock_df
    def get_history_bill_multi(self,df2: pd.DataFrame, tries,ktl:int, **kwargs) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
          获取多只只股票历史单子流入流出数据

          Parameters
          ----------
          df2 : pd.DataFrame,
              含有股票代码的DataFrame

          Returns
          -------
          DataFrame
              沪深市场单只股票历史单子流入流出数据

          Examples
          --------
          # >>> import efinance as ef
          # >>> ef.stock.get_history_bill('600519')
              股票名称    股票代码          日期         主力净流入       小单净流入         中单净流入         大单净流入        超大单净流入  主力净流入占比  小单流入净占比  中单流入净占比  大单流入净占比  超大单流入净占比      收盘价   涨跌幅
          0    贵州茅台  600519  2021-03-04 -3.670272e+06  -2282056.0  5.952143e+06  1.461528e+09 -1.465199e+09    -0.03    -0.02     0.04    10.99    -11.02  2013.71 -5.05
          1    贵州茅台  600519  2021-03-05 -1.514880e+07  -1319066.0  1.646793e+07 -2.528896e+07  1.014016e+07    -0.12    -0.01     0.13    -0.19      0.08  2040.82  1.35
          2    贵州茅台  600519  2021-03-08 -8.001702e+08   -877074.0  8.010473e+08  5.670671e+08 -1.367237e+09    -6.29    -0.01     6.30     4.46    -10.75  1940.71 -4.91
          3    贵州茅台  600519  2021-03-09 -2.237770e+08  -6391767.0  2.301686e+08 -1.795013e+08 -4.427571e+07    -1.39    -0.04     1.43    -1.11     -0.27  1917.70 -1.19
          4    贵州茅台  600519  2021-03-10 -2.044173e+08  -1551798.0  2.059690e+08 -2.378506e+08  3.343331e+07    -2.02    -0.02     2.03    -2.35      0.33  1950.72  1.72
          ..    ...     ...         ...           ...         ...           ...           ...           ...      ...      ...      ...      ...       ...      ...   ...
          97   贵州茅台  600519  2021-07-26 -1.564233e+09  13142211.0  1.551091e+09 -1.270400e+08 -1.437193e+09    -8.74     0.07     8.67    -0.71     -8.03  1804.11 -5.05
          98   贵州茅台  600519  2021-07-27 -7.803296e+08 -10424715.0  7.907544e+08  6.725104e+07 -8.475807e+08    -5.12    -0.07     5.19     0.44     -5.56  1712.89 -5.06
          99   贵州茅台  600519  2021-07-28  3.997645e+08   2603511.0 -4.023677e+08  2.315648e+08  1.681997e+08     2.70     0.02    -2.72     1.57      1.14  1768.90  3.27
          100  贵州茅台  600519  2021-07-29 -9.209842e+08  -2312235.0  9.232964e+08 -3.959741e+08 -5.250101e+08    -8.15    -0.02     8.17    -3.50     -4.65  1749.79 -1.08
          101  贵州茅台  600519  2021-07-30 -1.524740e+09  -6020099.0  1.530761e+09  1.147248e+08 -1.639465e+09   -11.63    -0.05    11.68     0.88    -12.51  1678.99 -4.05

          """
        multitasking.set_max_threads(multitasking.config["CPU_CORES"] * 2)
        dfs: Dict[str, pd.DataFrame] = {}
        codes=df2['股票代码'].tolist()
        total = len(codes)
        pbar = tqdm(total=total, bar_format="{l_bar}%s{bar}%s{r_bar}" % (Fore.GREEN, Fore.CYAN))
        @multitasking.task
        @retry(tries=tries, delay=1)
        def start(code):
            _start={15:ef.stock.get_today_bill,101:ef.stock.get_history_bill,200:self.get_deal_detail,300:self.get_stock_quote}
            _df = _start.get(ktl)(code)
            dfs[code] = _df
            pbar.set_description_str(f'Processing => {code}',False)
            pbar.set_postfix_str(f'下载完成', False)
            pbar.update(1)

        for code in codes:
            start(code)
        multitasking.wait_for_tasks()
        pbar.close()
        if kwargs.get('return_df'):
            return pd.concat(dfs, axis=0, ignore_index=True)
        return dfs

    def generate_data(self,days_to_subtract,extract_all=False):
        if not os.path.exists('src'):
            # 如果目录不存在，则创建它
            os.makedirs('src')

        realtime_stock_info = self.get_realtime_stock_info()
        inno_company_info = self.get_inno_company("b:BK1005")
        filtered_realtime_stock_info = self.filter_realtime_stock_info(realtime_stock_info)
        filtered_realtime_stock_info.to_parquet('./src/realtime.parquet')
        if extract_all:
            filtered_realtime_stock_info = realtime_stock_info
        stock_data_15min = self.get_quote_history(filtered_realtime_stock_info,days_to_subtract,15)
        stock_data_daily = self.get_quote_history(filtered_realtime_stock_info, days_to_subtract, 101)
        fund_flow_history=self.get_history_bill_multi(filtered_realtime_stock_info,3,ktl=101,return_df=True)
        fund_flow_minute = self.get_history_bill_multi(filtered_realtime_stock_info, 3, ktl=15, return_df=True)
        stock_intraday_volume = self.get_history_bill_multi(filtered_realtime_stock_info, 3, ktl=200, return_df=True)
        stock_chip_plate = self.get_history_bill_multi(filtered_realtime_stock_info, 3, ktl=300, return_df=True)
        # 保存数据到 Parquet 文件和 Excel 文件


        realtime_stock_info.to_parquet('./src/all_realtime.parquet')
        inno_company_info.to_parquet('./src/inno.parquet')

        stock_data_15min.to_parquet('./src/stock_data_15min.parquet')
        stock_data_daily.to_parquet('./src/stock_data_daily.parquet')
        stock_intraday_volume.to_parquet('./src/stock_intraday_volume.parquet')
        stock_chip_plate.to_parquet('./src/stock_chip_plate.parquet')

        fund_flow_history.to_parquet('./src/fund_flow_history.parquet')
        fund_flow_minute.to_parquet('./src/fund_flow_minute.parquet')


        # database_handler = DuckDBHandler()
        # with database_handler.connection as conn:
            # filtered_realtime_stock_info.to_sql('realtime', conn, index=False, if_exists='replace', method='multi', chunksize=10000)
            # del filtered_realtime_stock_info
            # realtime_stock_info.to_sql('all_realtime', conn, index=False, if_exists='replace', method='multi', chunksize=10000)
            # del realtime_stock_info
            # quote_history.to_sql('data', conn, index=False, if_exists='replace', method='multi', chunksize=10000)
            # del quote_history
            # inno_company_info.to_sql('inno', conn, index=False, if_exists='replace', method='multi', chunksize=10000)
            # del inno_company_info

        # database_handler.close_connection()
if __name__ == '__main__':
    analyzer = StockAnalyzer()
    analyzer.generate_data()
