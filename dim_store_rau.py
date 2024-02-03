import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT store_code, store, substr(store_code,0,4) city_code  FROM item_sales GROUP BY store_code, store ORDER BY store_code"
stores = pd.read_sql_query(query,connection)
stores = stores.reset_index(drop=False)
stores = stores.rename(columns={'index':'id_store'})

query = "SELECT id_city, city_code FROM DIM_CITIES"
cities = pd.read_sql_query(query,connection)
result = pd.merge(stores, cities, on='city_code', how='inner')
result = result.drop('city_code', axis=1)

result.to_sql(name='DIM_STORES', con=connection, if_exists='replace', index=False)
connection.commit()

connection.close()
print('Desconexión')