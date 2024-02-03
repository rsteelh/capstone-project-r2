import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT distinct substr(store_code,0,4) as city_code, region as city FROM item_sales ORDER BY store_code"
cities = pd.read_sql_query(query,connection)
cities = cities.reset_index(drop=False)
cities = cities.rename(columns={'index':'id_city'})

cities.to_sql(name='DIM_CITIES', con=connection, if_exists='replace', index=False)
connection.commit()

connection.close()
print('Desconexión')