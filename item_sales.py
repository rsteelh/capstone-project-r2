#conexión base de datos SQL Lite
import sqlite3
import pandas as pd
import sys

ruta_db = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"
ruta_csv = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/item_sales.csv"

try:
  connection = sqlite3.connect(ruta_db)
  print('Conexión establecida')
except:
  print('Error al intentar la conexión')

#importación ficheros
ventas = pd.read_csv(ruta_csv)

#normalización dataframe
ventas_con_ceros=ventas.melt(id_vars=['id', 'item', 'category', 'department','store', 'store_code', 'region'], var_name='d',value_name='sales')

print(ventas_con_ceros["sales"].sum())
len(ventas)

#Exploración y limpieza
#borrado de ventas con valor 0
indices_eliminar=ventas_con_ceros[ventas_con_ceros['sales'] == 0].index
total = len(ventas_con_ceros)-len(indices_eliminar)
ventas = ventas_con_ceros.drop(indices_eliminar)
print(len(ventas))
del ventas_con_ceros

#miramos duplicados
duplicados = ventas[ventas.duplicated(keep=False)]
if len(duplicados) != 0 :
  print("Tratar duplicados")

#tratamos nulos
item = ventas['item'].isnull().any()
store_code = ventas['store_code'].isnull().any()
if item == True | store_code == True:
  print("Tratar nulos")


print(len(ventas))
#volcamos dataframe en la base de datos
ventas.reset_index(drop=True, inplace=True)
ventas.to_sql(name='item_sales', con=connection, if_exists='replace', index=False)
del ventas
connection.close()
print('Desconexión')
sys.exit()





'''
#HACIENDOLO CON BUCLES FOR
ventas = pd.read_csv(ruta_csv)
#ordernamos el dataframe por item (no por id)
ventas = ventas.sort_values(by='item', ascending=True)

#Exploración y limpieza
#miramos duplicados
duplicados = ventas[ventas.duplicated(keep=False)]
if len(duplicados) != 0 :
  print("Tratar duplicados")

#tratamos nulos
item = ventas['item'].isnull().any()
store_code = ventas['store_code'].isnull().any()
if item == True | store_code == True:
  print("Tratar nulos")


#miramos los datos ya cargados
q = """ SELECT DISTINCT(item) FROM item_sales ORDER BY 1 """
try:
  #creamos lista de items ya cargados para no tener que repetir la carga si esta se interrumpe y seguir por el sitio que acabó
  productos_cargados = pd.read_sql_query(q, connection)
  #miramos cual es el último que hay en la lista ordenada alfabéticamente
  ult_producto = productos_cargados.tail(1)['item'].values[0]
  # Borramos de la base de datos el último valor para no duplicarlo:
  qd = f"DELETE FROM item_sales WHERE item='{ult_producto}'"
  cursor = connection.cursor()
  filas_borradas = cursor.execute(qd).rowcount
  connection.commit;
  print(f"Borradas {filas_borradas} filas del último producto {ult_producto}")
  cursor.close()
  #recalculamos productos borradows despues del borrado, en teoria los productos cargados menos 1
  productos_cargados_dp = productos_cargados[:len(productos_cargados)-1]
except:
  #capturamos el error si la tabla no existe e inicializamos los df con 0 filas
  columna=['item']
  productos_cargados_dp = pd.DataFrame(columns=columna)

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
dies = len(ventas.columns[7:].tolist())
#productos diferentes
productos = ventas['item'].unique()
max_ventas = len(ventas)
x = 0
tabla_existe=0

#bucle de ventas
for i in range(max_ventas):
  item_actual = ventas.iloc[i, 1]
  #miramos si la fila con el producto actual ventas.iloc[i, 1] ya ha sido cargado
  cargado = productos_cargados_dp[productos_cargados_dp['item'] == item_actual]
  #si la fila está vacia quiere decir que no esta ya cargada y empezamos el bucle de carga
  if cargado.empty == True:
    for j in range(dies):
      #no cargamos las ventas 0
      if (ventas.iloc[i, 7 + j] != 0):
        ventas2.loc[x] = [ventas.iloc[i, 1], ventas.iloc[i, 5], ventas.columns[7+j], ventas.iloc[i, 7+j]]
      x += 1
    # Verificamos si tenemos que crear la tabla o la tenemos ya lista para seguri añadiendo filas
    if len(productos_cargados) == 0 and tabla_existe == 0:
      ventas2.drop(ventas2.index, inplace=True)
      ventas2.to_sql(name='item_sales', con=connection, if_exists='replace', index=False)
      x = 0
      tabla_existe = 1
      print("producto cargado:", ventas.iloc[i, 1], "tienda: ", ventas.iloc[i, 5])
      connection.commit()
    else:
      ventas2.to_sql(name='item_sales', con=connection, if_exists='append', index=False)
      ventas2.drop(ventas2.index, inplace=True)
      x = 0
      print("producto cargado:", ventas.iloc[i, 1], "tienda: ", ventas.iloc[i, 5])
      connection.commit()

connection.close()
print('Desconexión')
'''