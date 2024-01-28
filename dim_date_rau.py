import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive/Documentos/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT * FROM daily_calendar"
date = pd.read_sql_query(query,connection)
date = date.drop('d', axis=1)

date.to_sql(name='DIM_DATE', con=connection, if_exists='replace', index=False)
connection.commit()

connection.close()
print('Desconexión')