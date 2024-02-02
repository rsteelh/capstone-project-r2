#conexión base de datos SQL Lite
import sqlite3

try:
  connection = sqlite3.connect("C:/Users/rsalcedo/OneDrive/Documentos/projects_ds/dsmarket/dsmarket1/data/dsmarket.db")
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')
try:
    cursor = connection.cursor()
    cursor.execute("DROP TABLE item_prices;")
except:
   print("la tabla no existe")

import pandas as pd
#importación ficheros
columna_tipos = {'item': 'str', 'category': 'str', 'store_code': 'str', 'yearweek': 'str', 'sell_price': 'float'}
precios = pd.read_csv("C:/Users/rsalcedo/OneDrive/Documentos/projects_ds/dsmarket/dsmarket1/data/item_prices.csv", dtype=columna_tipos)

#Exploración y limpieza
#miramos duplicados
print(precios.isnull().sum())
#no existen, tratamos nulos
item = precios['item'].isnull().any()
category = precios['category'].isnull().any()
store_code = precios['store_code'].isnull().any()
yearweek = precios['yearweek'].isnull().any()
sell_price = precios['sell_price'].isnull().any()
if item == True:
    print("tratar nulos item")
    precios = precios.dropna(subset=['item'])
if store_code == True:
    print("tratar nulos store_code")
    precios = precios.dropna(subset=['store_code'])
if category == True:
    print("tratar nulos category")
    precios = precios.dropna(subset=['category'])
if yearweek == True:
    print("tratar nulos yearweek")
    precios = precios.dropna(subset=['yearweek'])
if sell_price == True:
    print("Tratar nulos sell_price")
    precios = precios.dropna(subset=['sell_price'])

#creación de df
precios.to_sql(name='item_prices', con=connection, if_exists='replace', index=False)

connection.commit()
connection.close()
print('Desconexión')