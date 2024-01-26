#conexión base de datos SQL Lite
import sqlite3

try:
  connection = sqlite3.connect("C:/Users/rsalcedo/OneDrive/Documentos/projects_ds/dsmarket/dsmarket1/data/dsmarket.db")
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

import pandas as pd
#importación ficheros
#calendario = pd.read_csv("C:/capstone/daily_calendar_with_events.csv")
#precios = pd.read_csv("C:/capstone/item_prices.csv")
ventas = pd.read_csv("C:/Users/rsalcedo/OneDrive/Documentos/projects_ds/dsmarket/dsmarket1/data/item_sales.csv")
#ordernamos el dataframe por item (no por id)
ventas = ventas.sort_values(by='item', ascending=True)

#Exploración y limpieza
#miramos duplicados
print(ventas[ventas.duplicated(keep=False)])
#no existen, tratamos nulos
item = ventas['item'].isnull().any()
store_code = ventas['store_code'].isnull().any()
if item == True | store_code == True:
  print("Tratar nulos")

#miramos los datos ya cargados
q = """
SELECT DISTINCT(item)
FROM item_sales
ORDER BY 1
"""
try:
  #creamos lista de items ya cargados para no tener que repetir la carga si esta se interrumpe y seguir por el sitio que acabó
  productos_cargados = pd.read_sql_query(q, connection)
  #miramos cual es el último que hay en la lista ordenada alfabéticamente
  ult_producto = productos_cargados.tail(1)['item'].values[0]
  #borramos el último por si el bucle se quedó a medias
  productos_cargados2 = productos_cargados[:len(productos_cargados)-1]
  # Borramos de la base de datos el último valor para no duplicarlo:
  cursor = connection.cursor()
  q = f"DELETE FROM item_sales WHERE item='{ult_producto}'"
  filas_borradas = cursor.execute(q).rowcount
  print(f"Borradas {filas_borradas} filas del último producto {ult_producto}")
  connection.commit;
except:
  #capturamos el error si la tabla no existe e inicializamos los df con 0 filas
  columna=['item']
  productos_cargados = pd.DataFrame(columns=columna)
  productos_cargados2 = pd.DataFrame(columns=columna)

#creación de df
#ventas2 = pd.DataFrame(columns=ventas.columns[:7].tolist())
columnas=['item', 'store_code', 'd', 'sales']
ventas2 = pd.DataFrame(columns=columnas)
ventas2['item'] = ventas2['item'].astype('string')
ventas2['store_code'] = ventas2['store_code'].astype('string')
ventas2['d'] = ventas2['d'].astype('string')
ventas2['sales'] = ventas2['sales'].astype('int8')
#ventas2[["Dia", "Ventas"]] = None

#dias del df
dias = ventas.columns[7:].tolist()
max_dies = len(dias)
#productos diferentes
lista_productos = ventas['id'].unique()
max_productos = len(lista_productos) - len(productos_cargados2)
x = 0
tabla_existe=0

for i in range(max_productos):
  item_actual = ventas.iloc[i, 1]
  #miramos si la fila con el producto actual ventas.iloc[i, 1] ya ha sido cargado
  cargado = productos_cargados2[productos_cargados2['item'] == ventas.iloc[i, 1]]
  #si la fila está vacia quiere decir que no esta ya cargada y empezamos el bucle de carga
  if cargado.empty == True:
    for j in range(max_dies):
      #no cargamos las ventas 0
      if (ventas.iloc[i, 7 + j] != 0 | ventas.iloc[i, 7 + j] == null):
        ventas2.loc[x] = [ventas.iloc[i, 1], ventas.iloc[i, 5], ventas.columns[7+j], ventas.iloc[i, 7+j]]
      x += 1
    # Verificamos si tenemos que crear la tabla o la tenemos ya lista para seguri añadiendo filas
    if len(productos_cargados) == 0 and tabla_existe == 0:
      ventas2.to_sql(name='item_sales', con=connection, if_exists='replace', index=False)
      ventas2.drop(ventas2.index, inplace=True)
      x = 0
      tabla_existe = 1
      print("producto cargado:",ventas.iloc[i, 1], "tienda: ", ventas.iloc[i, 5])
      connection.commit()
    else:
      ventas2.to_sql(name='item_sales', con=connection, if_exists='append', index=False)
      ventas2.drop(ventas2.index, inplace=True)
      x = 0
      print("producto cargado:", ventas.iloc[i, 1], "tienda: ", ventas.iloc[i, 5])
      connection.commit()

connection.close()
print('Desconexión')