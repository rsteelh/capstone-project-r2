import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT id_item,  sum(sales) as sales, sum(sales*sell_price) as total FROM FACT_SALES GROUP BY id_item"
data = pd.read_sql_query(query,connection)
data.to_sql(name='VM_ITEMS_SOLD', con=connection, if_exists='replace', index=False)
del data
connection.commit()

query = "SELECT id_item, id_store, sum(sales)as  sales, sum(sales*sell_price) as total FROM FACT_SALES GROUP BY id_item, id_store"
data = pd.read_sql_query(query,connection)
data.to_sql(name='VM_ITEMS_SOLD_BY_STORE', con=connection, if_exists='replace', index=False)
del data
connection.commit()

connection.close()
print('Desconexión')