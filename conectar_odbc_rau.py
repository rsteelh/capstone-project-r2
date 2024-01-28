import pyodbc

# Configura la cadena de conexión con el nombre del DSN que creaste
cadena_conexion = 'DSN=DS_Market'

try:
    # Conectar a la base de datos SQLite
    conexion = pyodbc.connect(cadena_conexion)
    
    # Crear un cursor para ejecutar consultas SQL
    cursor = conexion.cursor()

    # Ejemplo: ejecutar una consulta SELECT
    cursor.execute('SELECT * FROM DIM_DATE')
    
    # Obtener los resultados
    resultados = cursor.fetchall()
    
    # Imprimir los resultados
    for fila in resultados:
        print(fila)

except Exception as e:
    print(f"Error: {e}")

finally:
    # Cerrar la conexión al finalizar
    if 'conexion' in locals():
        conexion.close()