#conexión base de datos SQL Lite
import sqlite3

ruta_db = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"
ruta_csv = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/item_prices.csv"

try:
  connection = sqlite3.connect(ruta_db)
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
precios = pd.read_csv(ruta_csv, dtype=columna_tipos)

print("numero de registros:",len(precios))
precios_nonulos = precios.dropna(subset=['yearweek'])
print("numero de registros no nulos:",len(precios_nonulos))

#Exploración y limpieza
#miramos duplicados
print(precios['yearweek'].isnull().sum())
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
    #tratamos nulos de yearweek
    # creamos lista cursor de tiendas
    lista_tiendas = precios['store_code'].unique()
    # creamos lista cursor de items
    lista_items = precios['item'].unique()
    # creamos la lista de semanas que queremos rellenar
    query = "select distinct yearweek from daily_calendar order by yearweek"
    rango_semanas = pd.read_sql_query(query, connection)
    # creamos dataset para ir llenando valores
    yearweek_nulos = pd.DataFrame()
    #bucle de stores
    for i in range(len(lista_tiendas)):
        #bucle de items
        for j in range(len(lista_items)):
            print("dupla",lista_tiendas[i],lista_items[j])
            # hacer un subset con los datos del bucle y ordenarlos por fecha
            precios_subset = precios[(precios["store_code"] == lista_tiendas[i]) & (precios["item"] == lista_items[j])]
            # miramos si el subset tiene nulos
            if precios_subset['yearweek'].isnull().any() == True:
                # creamos data set sin nulos para calcular la ultima semana
                precios_subset_no_nulos = precios_subset.dropna(subset=['yearweek'])
                # creamos data con los yearweek nulos a tratar
                precios_subset_nulos = precios_subset[precios_subset['yearweek'].isna()]
                #calculamos la última semana con dato
                min_yearweek = precios_subset_no_nulos['yearweek'].min()
                max_yearweek = precios_subset_no_nulos['yearweek'].max()
                # creamos dataframe con los valores de yearweek menores y reordenamos el indice qpara que empiece por el valor más alto
                rango_semanas_x = rango_semanas[rango_semanas['yearweek'] < min_yearweek].sort_values(by='yearweek',ascending=False).reset_index(drop=True)
                rango_semanas_y = rango_semanas[rango_semanas['yearweek'] > max_yearweek].sort_values(by='yearweek',ascending=True).reset_index(drop=True)
                #calculamos el maximo numero de semanas que nos quedan menores en daily calendar
                max_num_yearweek_x = len(rango_semanas_x)
                max_num_yearweek_y = len(rango_semanas_y)
                # reseteamos el indice para poder escribir al mismo nivel que reango_semanas_x
                precios_subset_nulos.reset_index(drop=True, inplace=True)
                #bucle per yearweek nulo
                if max_num_yearweek_x > max_num_yearweek_y:
                    for k in range(len(precios_subset_nulos)):
                    #verificamos que nos queden semanas para asignar en daily_calendar
                        if max_num_yearweek_x >= k:
                            #asignamos el valor de yearweek al registro
                            precios_subset_nulos.loc[k, 'yearweek'] = rango_semanas_x.loc[k, 'yearweek']
                        max_num_yearweek_x-= 1
                else:#restamos la semana asignada
                    for k in range(len(precios_subset_nulos)):
                        if max_num_yearweek_y >= k:
                            precios_subset_nulos.loc[k, 'yearweek'] = rango_semanas_y.loc[k, 'yearweek']
                        max_num_yearweek_y -= 1
                #borramos aquelos registros que hayan qudado sin asignar por falta de registros
                precios_subset_nulos=precios_subset_nulos.dropna(subset=['yearweek'])
                #concatenamos al dataframe de nulos
                yearweek_nulos = pd.concat([yearweek_nulos, precios_subset_nulos], ignore_index=True)
if sell_price == True:
    print("Tratar nulos sell_price")
    precios = precios.dropna(subset=['sell_price'])

precios = precios.dropna(subset=['yearweek'])
precios_total = pd.concat([precios, yearweek_nulos], ignore_index=True)
#creación de df
precios_total.to_sql(name='item_prices', con=connection, if_exists='replace', index=False)

connection.commit()
connection.close()
print('Desconexión')