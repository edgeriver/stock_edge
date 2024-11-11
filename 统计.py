from stock_handler.duckdb_handler import DuckDBHandler
from stock_handler import sqlScripts

db_handler = DuckDBHandler('quant_data.db')

df=db_handler.execute_query(sqlScripts.stocks_analysis)
db_handler.close_connection()
print(df)