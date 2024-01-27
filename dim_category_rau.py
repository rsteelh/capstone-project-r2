import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive/Documentos/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT distinct category FROM item_prices ORDER BY category"
cities = pd.read_sql_query(query,connection)
cities = cities.reset_index(drop=False)
cities = cities.rename(columns={'index':'id_category'})

cities.to_sql(name='DIM_CATEGORIES', con=connection, if_exists='replace', index=False)
connection.commit()

connection.close()
print('Desconexión')