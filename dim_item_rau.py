import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive/Documentos/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT distinct item, category FROM item_prices ORDER BY item"
items = pd.read_sql_query(query,connection)
items = items.reset_index(drop=False)
items = items.rename(columns={'index':'id_item'})

query = "SELECT * FROM DIM_CATEGORIES"
categories = pd.read_sql_query(query,connection)
items = pd.merge(items, categories, on='category', how='inner')
items = items.drop('category', axis=1)

items.to_sql(name='DIM_ITEMS', con=connection, if_exists='replace', index=False)
connection.commit()

connection.close()
print('Desconexión')
