from stock_handler.duckdb_handler import DuckDBHandler
from stock_handler import sqlScripts

db_handler = DuckDBHandler('quant_data.db')

db_handler.execute_query(sqlScripts.init_indicator)
db_handler.execute_query(sqlScripts.stock_data_update_task)
df=db_handler.execute_query(sqlScripts.stocks_analysis)
print(df)
db_handler.close_connection()