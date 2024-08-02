import duckdb
import pandas as pd

class DuckDBHandler:
    def __init__(self, database_path):
        self.connection = duckdb.connect(database_path)

    def execute_query(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchdf()
        cursor.close()
        return result

    def insert_data(self, table_name, data):
        columns = ', '.join(data.keys())
        values = ', '.join([f"'{value}'" for value in data.values()])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        self.execute_query(query)

    def update_data(self, table_name, data, condition):
        set_values = ', '.join([f"{key} = '{value}'" for key, value in data.items()])
        where_condition = ' AND '.join([f"{key} = '{value}'" for key, value in condition.items()])
        query = f"UPDATE {table_name} SET {set_values} WHERE {where_condition}"
        self.execute_query(query)

    def delete_data(self, table_name, condition):
        where_condition = ' AND '.join([f"{key} = '{value}'" for key, value in condition.items()])
        query = f"DELETE FROM {table_name} WHERE {where_condition}"
        self.execute_query(query)
   
    def register(self,tablename:str,df:pd.DataFrame):
        self.connection.register(view_name=tablename, python_object=df)

    def close_connection(self):
        self.connection.close()


# 示例用法
if __name__ == '__main__':
    db_handler = DuckDBHandler('../example.db')
    db_handler.execute_query('''    CREATE TABLE  IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            age INTEGER
        )''')
    # 插入数据
    data_to_insert = {'id': 1, 'name': 'Alice', 'age': 30}
    db_handler.insert_data('users', data_to_insert)

    # 更新数据
    data_to_update = {'name': 'Bob', 'age': 35}
    update_condition = {'id': 1}
    db_handler.update_data('users', data_to_update, update_condition)

    # # 删除数据
    # delete_condition = {'id': 1}
    # db_handler.delete_data('users', delete_condition)

    db_handler.close_connection()

