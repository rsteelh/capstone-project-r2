import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT item, category, department, store_code, d, sales FROM item_sales"
sales = pd.read_sql_query(query,connection)
print(len(sales))
query = "SELECT id_item, item FROM DIM_ITEMS"
items = pd.read_sql_query(query,connection)
result = pd.merge(sales, items, on='item', how='inner')
print(len(result))
del items
del sales
print("Primera etapa productos")

query = "SELECT d, date, yearweek FROM daily_calendar"
fechas = pd.read_sql_query(query,connection)
#fechas['id_date'] = fechas['date']
result2 = pd.merge(result, fechas, on='d', how='inner')
print(len(result2))
del fechas
del result
col_to_drop = ['date']
result2 = result2.drop(col_to_drop, axis=1)
print("Segunda etapa fechas")

query = "SELECT id_store, store_code, id_city FROM DIM_STORES"
stores = pd.read_sql_query(query,connection)
result3 = pd.merge(result2, stores, on='store_code', how='inner')
print(len(result3))
del stores
del result2
print("Tercera etapa tiendas")

query = "SELECT id_department, department FROM DIM_DEPARTMENT"
departments = pd.read_sql_query(query,connection)
result4 = pd.merge(result3, departments, on='department', how='inner')
print(len(result4))
del departments
del result3
result4 = result4.drop('department', axis=1)
print("Cuarta etapa departamentos")

query = "SELECT id_category, category FROM DIM_CATEGORIES"
categories = pd.read_sql_query(query,connection)
result5 = pd.merge(result4, categories, on='category', how='inner')
print(len(result5))
del categories
del result4
result5 = result5.drop('category', axis=1)
print("Quinta etapa Categorias")

query = "SELECT yearweek, store_code, item, sell_price FROM item_prices"
prices = pd.read_sql_query(query,connection)
result6 = pd.merge(result5,prices, left_on=['yearweek','store_code','item'], right_on=['yearweek','store_code','item'], how='inner')
print(len(result6))
del prices
del result5
col_to_drop = ['item','yearweek','store_code']
result6 = result6.drop(col_to_drop, axis=1)
#cálculos finales
result6 = result6.reset_index(drop=False)
#result6['total'] = result6['sales']*result6['sell_price']
#result6 = result6.rename(columns={'index':'id_sale'})
print("Ultima etapa precios")

result6.to_sql(name='FACT_SALES', con=connection, if_exists='replace', index=False)
del result6

"""result7 = pd.merge(result5,prices, left_on=['yearweek','store_code','item'], right_on=['yearweek','store_code','item'], how='left')
print(len(result7))
col_to_drop = ['item','yearweek','store_code']
result7 = result7.drop(col_to_drop, axis=1)
result7 = result7.reset_index(drop=False)
result7.to_sql(name='FACT_SALES_LEFT', con=connection, if_exists='replace', index=False)
del result7"""

connection.commit()
connection.close()
print('Desconexión')