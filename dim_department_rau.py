import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT distinct category, department FROM item_sales order by category"
department = pd.read_sql_query(query,connection)
query = "SELECT id_category, category FROM DIM_CATEGORIES"
category = pd.read_sql_query(query,connection)
result = pd.merge(department, category, on='category', how='inner')
result = result.reset_index(drop=False)
result = result.rename(columns={'index':'id_department'})
col_to_drop = ['category']
result2 = result.drop(col_to_drop, axis=1)
del result

result2.to_sql(name='DIM_DEPARTMENT', con=connection, if_exists='replace', index=False)
connection.commit()
del result2

connection.close()
print('Desconexión')