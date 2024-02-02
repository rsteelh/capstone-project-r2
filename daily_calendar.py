#conexion base de datos SQL Server
#import pyodbc
#from sqlalchemy import create_engine
#server = 'PREVF-SQL03\\MINERVA'
#database = 'DSMarket'
#username = 'raul'
#password = '4321'
#conn_str = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=SQL+Server'
#try:
#  engine = create_engine(conn_str, use_setinputsizes=False)
#  connection = engine.connect()
#  print('Conexión establecida')
#except:
#  print('Error al intentar la conexión')


#conexión base de datos SQL Lite
import sqlite3

try:
  connection = sqlite3.connect("C:/Users/rsalcedo/OneDrive/Documentos/projects_ds/dsmarket/dsmarket1/data/dsmarket.db")
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')
try:
    cursor = connection.cursor()
    cursor.execute("DROP TABLE daily_calendar;")
except:
   print("la tabla no existe")

import pandas as pd
#importación ficheros
columna_tipos = {'date': 'str', 'weekday': 'str', 'weekday_int': 'int', 'd': 'str', 'event': 'str'}
calendario = pd.read_csv("C:/Users/rsalcedo/OneDrive/Documentos/projects_ds/dsmarket/dsmarket1/data/daily_calendar_with_events.csv", dtype=columna_tipos)

#miramos duplicados
print(calendario.isnull().sum())

#miramos nulos
date = calendario['date'].isnull().any()
weekday = calendario['weekday'].isnull().any()
weekday_int = calendario['weekday_int'].isnull().any()
d = calendario['d'].isnull().any()
#no hay nulos, solo en events

calendario['yearweek'] = calendario['date'].str.slice(0, 4) + calendario['date'].str.slice(5, 7)

calendario['date'] = pd.to_datetime(calendario['date']).apply(lambda x: x.strftime('%Y-%m-%d'))

#creación de df
calendario.to_sql(name='daily_calendar', con=connection, if_exists='replace', index=False)
connection.commit()

connection.close()
print('Desconexión')