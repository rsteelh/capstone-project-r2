import sqlite3
import pandas as pd

ruta_db = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

query = "SELECT * FROM daily_calendar"
date = pd.read_sql_query(query,connection)
date['id_date'] = pd.to_datetime(date['date'])
date['year'] = (date['id_date'].dt.strftime('%Y'))
date = date.drop('date', axis=1)
#date = date.drop('d', axis=1)

date.to_sql(name='DIM_DATE', con=connection, if_exists='replace', index=False)
connection.commit()

connection.close()
print('Desconexión')