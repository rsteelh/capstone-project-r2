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
query = "SELECT d, date, yearweek FROM daily_calendar"
fechas = pd.read_sql_query(query,connection)
#creamos un campo de tipo fecha
fechas['fecha'] = pd.to_datetime(fechas['date'], format='%Y-%m-%d')
fechas = fechas.drop('date', axis=1)
result = pd.merge(sales, items, on='item', how='inner')
result2 = pd.merge(result, fechas, on='d', how='inner')
del result

query = "SELECT * FROM DIM_STORES"
stores = pd.read_sql_query(query,connection)
result3 = pd.merge(result2, stores, on='store_code', how='inner')
del result2


query = "SELECT * FROM item_prices"
prices = pd.read_sql_query(query,connection)
result4 = pd.merge(result3,prices, left_on=['yearweek','store_code','item'], right_on=['yearweek','store_code','item'], how='inner')
result4 = result4.drop('store_code', axis=1)
result4 = result4.drop('d', axis=1)
result4 = result4.drop('item', axis=1)
result4 = result4.drop('yearweek', axis=1)


print(result4)

result4.to_sql(name='FACT_SALES', con=connection, if_exists='replace', index=False)
del result4
connection.commit()

connection.close()
print('Desconexión')