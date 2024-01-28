import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive/Documentos/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT * FROM item_sales"
sales = pd.read_sql_query(query,connection)
query = "SELECT * FROM DIM_ITEMS"
items = pd.read_sql_query(query,connection)
result = pd.merge(sales, items, on='item', how='inner')
del sales
print("Primera etapa")

query = "SELECT d, date, yearweek FROM daily_calendar"
fechas = pd.read_sql_query(query,connection)
fechas['id_date'] = pd.to_datetime(fechas['date'], format='%Y-%m-%d')
result2 = pd.merge(result, fechas, on='d', how='inner')
del result
del fechas
col_to_drop = ['d','date']
result2 = result2.drop(col_to_drop, axis=1)
print("Segunda etapa")

query = "SELECT * FROM DIM_STORES"
stores = pd.read_sql_query(query,connection)
result3 = pd.merge(result2, stores, on='store_code', how='inner')
del stores
del result2
print("Tercera etapa")

query = "SELECT * FROM item_prices"
prices = pd.read_sql_query(query,connection)
result4 = pd.merge(result3,prices, left_on=['yearweek','store_code','item'], right_on=['yearweek','store_code','item'], how='inner')
del prices
col_to_drop = ['item','yearweek','category','store_code']
result4 = result4.drop(col_to_drop, axis=1)
print("Cuarta etapa")

result4 = result4.reset_index(drop=False)
result4['total'] = result4['sales']*result4['sell_price']
result4 = result4.rename(columns={'index':'id_sale'})
print("Última etapa")

result4.to_sql(name='FACT_SALES', con=connection, if_exists='replace', index=False)
connection.commit()
del result4

connection.close()
print('Desconexión')