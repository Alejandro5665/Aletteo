import sqlite3
from datetime import datetime, timedelta

# Conectar a la base de datos o crearla si no existe
conexion = sqlite3.connect("Electricity_Data.db")
cursor = conexion.cursor()

# Crear la tabla para almacenar los datos
cursor.execute("""
CREATE TABLE IF NOT EXISTS tiempo (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    año INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    hora INTEGER NOT NULL,
    minuto INTEGER NOT NULL
)
""")
conexion.commit()

# Función para verificar si un año es bisiesto
def es_bisiesto(año):
    return año % 4 == 0 and (año % 100 != 0 or año % 400 == 0)

# Generar los datos desde 2018 hasta 2026
def generar_datos_tiempo():
    # Fecha inicial y final
    inicio = datetime(2018, 1, 1, 0, 0)
    fin = datetime(2050, 12, 31, 23, 45)
    delta = timedelta(minutes=15)  # Intervalos de 15 minutos

    # Avanzar en el tiempo y acumular los datos
    fecha_actual = inicio
    datos = []

    while fecha_actual <= fin:
        año = fecha_actual.year
        mes = fecha_actual.month
        dia = fecha_actual.day
        hora = fecha_actual.hour
        minuto = fecha_actual.minute
        datos.append((año, mes, dia, hora, minuto))
        fecha_actual += delta

    return datos

# Guardar los datos en la base de datos
def guardar_datos_en_bd(datos):
    cursor.executemany("""
    INSERT INTO tiempo (año, mes, dia, hora, minuto)
    VALUES (?, ?, ?, ?, ?)
    """, datos)
    conexion.commit()
    print(f"Se han guardado {len(datos)} registros en la base de datos.")

# Generar y guardar los datos
datos_tiempo = generar_datos_tiempo()
guardar_datos_en_bd(datos_tiempo)

# Consultar los primeros registros para verificar
cursor.execute("SELECT * FROM tiempo LIMIT 1000000")
resultados = cursor.fetchall()
for r in resultados:
    print(r)

# Cerrar la conexión
conexion.close()

#import sqlite3
#import pandas as pd

# Conectar a la base de datos SQLite
#conexion = sqlite3.connect("Electricity_Data.db")

# Leer la tabla completa en un DataFrame
#df = pd.read_sql_query("SELECT * FROM tiempo", conexion)

# Mostrar los primeros registros
#print(df.head())

# Cerrar la conexión
##conexion.close()
