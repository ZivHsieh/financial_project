# import pandas as pd
# from sqlalchemy import create_engine

# # MySQL 連線詳細資訊
# mysql_ip = "172.18.0.2"
# user = "airflow"
# password = "airflow"
# database = "financial"
# table_name = "stock_prices_rowdata"
# new_table_name = "rowdata_trans_closing"

# # 建立資料庫連線引擎
# engine = create_engine(f'mysql+pymysql://{user}:{password}@{mysql_ip}/{database}')

# # 從資料庫讀取資料
# query = f"SELECT * FROM {table_name}"
# data = pd.read_sql(query, con=engine)

# # 假設資料框架中有一個叫 '收盤價' 的欄位
# # 將資料轉置
# close = pd.DataFrame({k: d['最高價'] for k, d in data.groupby(data.index)}).transpose()
# close.index = pd.to_datetime(close.index)

# # 將轉置後的資料寫入新的資料表
# close.to_sql(new_table_name, con=engine, if_exists='replace', index=True)

# print(f"Data has been successfully written to the table '{new_table_name}'.")
