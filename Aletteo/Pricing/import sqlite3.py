import sqlite3

# Conectar a la base de datos
conexion = sqlite3.connect("Electricity_Data.db")
cursor = conexion.cursor()

# Consulta para extraer los datos
query = "SELECT * FROM tiempo"
cursor.execute(query)
resultados = cursor.fetchall()

# Ruta y nombre del archivo TXT
ruta_txt = "Electricity_Data.txt"

# Exportar los datos a un archivo TXT
with open(ruta_txt, "w") as archivo:
    # Escribir el encabezado de las columnas
    encabezado = [descripcion[0] for descripcion in cursor.description]
    archivo.write("\t".join(encabezado) + "\n")  # Separar columnas con tabuladores

    # Escribir los registros
    for fila in resultados:
        archivo.write("\t".join(map(str, fila)) + "\n")  # Convertir valores a texto

print(f"Datos exportados correctamente a {ruta_txt}")

# Cerrar la conexión
conexion.close()
