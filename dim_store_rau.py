import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive/Documentos/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT distinct store_code FROM item_prices ORDER BY store_code"
stores = pd.read_sql_query(query,connection)
stores = stores.reset_index(drop=False)
stores = stores.rename(columns={'index':'id_store'})

query = "SELECT distinct store_code, substr(store_code,0,4) city FROM item_prices ORDER BY store_code"
stores_cities = pd.read_sql_query(query,connection)
result = pd.merge(stores, stores_cities, on='store_code', how='inner')

query = "SELECT id_city, city FROM DIM_CITIES"
cities = pd.read_sql_query(query,connection)
result = pd.merge(result, cities, on='city', how='inner')
result = result.drop('city', axis=1)

result.to_sql(name='DIM_STORES', con=connection, if_exists='replace', index=False)
connection.commit()

connection.close()
print('Desconexión')