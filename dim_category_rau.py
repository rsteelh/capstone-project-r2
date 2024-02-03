import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT distinct category FROM item_prices ORDER BY category"
category = pd.read_sql_query(query,connection)
category = category.reset_index(drop=False)
category = category.rename(columns={'index':'id_category'})

category.to_sql(name='DIM_CATEGORIES', con=connection, if_exists='replace', index=False)
connection.commit()

connection.close()
print('Desconexión')