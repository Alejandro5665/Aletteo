# Este codigo sirve para completar los huecos null que dejan los indicadores que solo tienen valores horarios frente a los cuarto-horarios
# Este codigo borrara todos los datos existentes en la tabla de destino para poner la nueva informacion posteriormente

from sqlalchemy import create_engine, text
import pymysql
import pandas as pd

# 🔹 Configuración de la conexión a MySQL
usuario = "Alejandro"
contraseña = "1999alex111"
host = "localhost"
base_datos = "base_datos_mysql"

# Crear la conexión a MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{contraseña}@{host}/{base_datos}")

# 🔹 Definir el rango de fechas para la extracción
fecha_inicio = "2023-01-01 00:00:00"  # 📌 Modifica esta fecha según necesites
fecha_fin = "2025-12-31 23:59:59"  # 📌 Modifica esta fecha según necesites

print(f"📅 Extrayendo datos entre {fecha_inicio} y {fecha_fin}")

# 🔹 Obtener las columnas de la tabla de origen (`Extracciones_horarias`)
query_columnas_origen = "SHOW COLUMNS FROM Extracciones_horarias;"
df_columnas_origen = pd.read_sql(query_columnas_origen, engine)
columnas_origen = df_columnas_origen["Field"].tolist()
columnas_origen.remove("Fecha")  # Excluir la columna "Fecha"
print("📌 Columnas en la tabla 'Extracciones_horarias' (sin 'Fecha'):", columnas_origen)

# 🔹 Obtener las columnas de la tabla de destino (`Extracciones_horarias_tratadas`)
query_columnas_destino = "SHOW COLUMNS FROM Extracciones_horarias_tratadas;"
df_columnas_destino = pd.read_sql(query_columnas_destino, engine)
columnas_destino = df_columnas_destino["Field"].tolist()

# 🔹 Crear columnas faltantes en `Extracciones_horarias_tratadas`
columnas_faltantes = [col for col in columnas_origen if col not in columnas_destino]

if columnas_faltantes:
    with engine.begin() as connection:
        for col in columnas_faltantes:
            sql_alter = f"ALTER TABLE Extracciones_horarias_tratadas ADD COLUMN `{col}` FLOAT DEFAULT NULL;"
            connection.execute(text(sql_alter))
            print(f"➕ Columna agregada en 'Extracciones_horarias_tratadas': {col}")

# 🔹 Volver a obtener las columnas de la tabla de destino después de la actualización
df_columnas_destino = pd.read_sql(query_columnas_destino, engine)
columnas_destino = df_columnas_destino["Field"].tolist()

print("📌 Columnas finales en 'Extracciones_horarias_tratadas':", columnas_destino)

# 🔹 Cargar datos de la tabla de origen con filtro de fechas
query_datos = f"""
    SELECT * FROM Extracciones_horarias
    WHERE Fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}';
"""
df_tratado = pd.read_sql(query_datos, engine)

# Verificar si se obtuvieron datos
if df_tratado.empty:
    print("⚠️ No se encontraron datos en el rango de fechas seleccionado.")
else:
    print(f"✅ Datos extraídos correctamente ({len(df_tratado)} filas)")

# 🔹 Filtrar solo las columnas que existen en la tabla de destino
df_tratado = df_tratado[[col for col in df_tratado.columns if col in columnas_destino]]

# 🔹 Agregar columnas faltantes en `df_tratado` con valores NaN
for col in columnas_destino:
    if col not in df_tratado.columns:
        df_tratado[col] = None

print("✅ Columnas después del tratamiento:", df_tratado.columns.tolist())

# 🔹 Convertir tipos de datos
df_tratado["Fecha"] = pd.to_datetime(df_tratado["Fecha"], errors="coerce")

# 🔹 Aplicar tratamiento de valores NULL
df_tratado.sort_values("Fecha", inplace=True)  # Ordenar por fecha

for col in columnas_origen:  # Iterar por cada indicador
    df_tratado[col] = df_tratado.groupby(df_tratado["Fecha"].dt.floor("h"))[col].transform(
        lambda x: x.ffill() if x.first_valid_index() == x.index[0] else x
    )

# 🔹 Eliminar todos los datos previos en la tabla de destino
with engine.begin() as connection:
    connection.execute(text("DELETE FROM Extracciones_horarias_tratadas;"))
    print("🗑️ Se han eliminado todos los datos previos en 'Extracciones_horarias_tratadas'.")

# 🔹 Insertar datos en MySQL correctamente
df_tratado.to_sql(
    "Extracciones_horarias_tratadas",
    con=engine,
    if_exists="append",  # Insertar después de borrar los datos previos
    index=False,
    method="multi"  # Inserción eficiente por lotes
)

print("✅ Datos insertados correctamente en la base de datos.")
