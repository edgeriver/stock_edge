from stock_handler.duckdb_handler import DuckDBHandler
from stock_handler.stock_analyzer import StockAnalyzer
from stock_handler import sqlScripts
analyzer = StockAnalyzer()
analyzer.generate_data(200,True)
db_handler = DuckDBHandler('quant_data.db')

db_handler.execute_query(sqlScripts.init_indicator)
# db_handler.execute_query(sqlScripts.init_indicator2)
# db_handler.execute_query(sqlScripts.obv_indicator)
# db_handler.execute_query(sqlScripts.rsi_calculation)
# db_handler.execute_query(sqlScripts.bollinger_indicator)
# db_handler.execute_query(sqlScripts.macd_model_calculation)

db_handler.execute_query(sqlScripts.stock_data_update_task)
df=db_handler.execute_query(sqlScripts.stocks_analysis)
print(df)
db_handler.close_connection()