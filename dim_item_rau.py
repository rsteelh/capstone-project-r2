import sqlite3
import pandas as pd

def substring(cadena, inicio, fin):
    return cadena[inicio:fin]
def item_short(row):
    return substring(row["item"], 0, 4)+ substring(row["item"], len(row["item"]) - 6, len(row["item"]))
def department_calculation(row):
        return substring(row["item"],0,row["item"].rfind("_"))

ruta_db = "C:/Users/rsalcedo/OneDrive - Generalitat de Catalunya/projects_ds/dsmarket/dsmarket1/data/dsmarket.db"

try:
    connection = sqlite3.connect(ruta_db)
    print('Conexión establecida')
except:
    print('Error al intentar la conexión')

query = "SELECT distinct item FROM item_prices ORDER BY item"
items = pd.read_sql_query(query, connection)
items = items.reset_index(drop=False)
items = items.rename(columns={'index': 'id_item'})
items["department"] = items.apply(department_calculation, axis=1)

query = "SELECT id_department, department FROM DIM_DEPARTMENT"
departments = pd.read_sql_query(query, connection)
items2 = pd.merge(items, departments, on='department', how='inner')
items2 = items2.drop('department', axis=1)
items2['item_short'] = items2.apply(item_short, axis=1)

items2.to_sql(name='DIM_ITEMS', con=connection, if_exists='replace', index=False)
connection.commit()

connection.close()
print('Desconexión')
