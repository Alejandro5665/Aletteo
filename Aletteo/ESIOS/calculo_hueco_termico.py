from sqlalchemy import create_engine, text
import pymysql
import pandas as pd
import numpy as np

# 🔹 Configuración de la conexión a MySQL
usuario = "Alejandro"
contraseña = "1999alex111"
host = "localhost"
base_datos = "base_datos_mysql"

# Crear la conexión a MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{contraseña}@{host}/{base_datos}")

# 🔹 Definir el rango de fechas y los indicadores que deseas extraer
fecha_inicio = "2023-01-01 00:00:00"  # 📌 Modifica esta fecha según necesites
fecha_fin = "2025-12-31 23:59:59"  # 📌 Modifica esta fecha según necesites

indicadores_deseados = [
    "Indicador_474",
    "Indicador_541",
    "Indicador_542",
    "Indicador_543",
    "Indicador_1844",
    "Indicador_1845",
    "Indicador_1846",
    "Indicador_1848",
    "Indicador_1849",
    "Indicador_1850",
    "Indicador_600",
]  # 📌 Agrega más indicadores si es necesario

print(f"📅 Extrayendo datos entre {fecha_inicio} y {fecha_fin} para indicadores: {indicadores_deseados}")

# 🔹 Obtener las columnas de `calculo_hueco_termico` para verificar si existen los indicadores
query_columnas_destino = "SHOW COLUMNS FROM calculo_hueco_termico;"
df_columnas_destino = pd.read_sql(query_columnas_destino, engine)
columnas_destino = df_columnas_destino["Field"].tolist()

# 🔹 Crear columnas faltantes en `calculo_hueco_termico`
columnas_faltantes = [col for col in indicadores_deseados if col not in columnas_destino]

if columnas_faltantes:
    with engine.begin() as connection:
        for col in columnas_faltantes:
            sql_alter = f"ALTER TABLE calculo_hueco_termico ADD COLUMN `{col}` FLOAT DEFAULT NULL;"
            connection.execute(text(sql_alter))
            print(f"➕ Columna agregada en 'calculo_hueco_termico': {col}")

# 🔹 Extraer datos desde `Extracciones_horarias_tratadas`
query_datos = f"""
    SELECT Fecha, {', '.join(indicadores_deseados)}
    FROM Extracciones_horarias_tratadas
    WHERE Fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}';
"""
df_datos = pd.read_sql(query_datos, engine)

# Verificar si se obtuvieron datos
if df_datos.empty:
    print("⚠️ No se encontraron datos en el rango de fechas seleccionado.")
else:
    print(f"✅ Datos extraídos correctamente ({len(df_datos)} filas)")

# 🔹 Manejo de valores NaT en la columna Fecha
print(f"🔍 Valores NaT en Fecha antes de limpieza: {df_datos['Fecha'].isna().sum()}")

# Convertir fechas y eliminar valores NaT
df_datos["Fecha"] = pd.to_datetime(df_datos["Fecha"], errors='coerce')  # Convierte fechas
df_datos = df_datos.dropna(subset=["Fecha"])  # Elimina filas con Fecha NaT
df_datos["Fecha"] = df_datos["Fecha"].dt.strftime("%Y-%m-%d %H:%M:%S")  # Formato para MySQL

# 🔹 Reemplazar NaN con None para evitar errores en MySQL
df_datos = df_datos.replace({np.nan: None})

# 🔹 Insertar o actualizar datos en `calculo_hueco_termico`
with engine.begin() as connection:
    for _, row in df_datos.iterrows():
        fecha = row["Fecha"]
        valores = {col: row[col] for col in indicadores_deseados}

        # Crear consulta SQL dinámica asegurando que None se convierta en NULL
        update_query = f"""
            INSERT INTO calculo_hueco_termico (Fecha, {', '.join(valores.keys())})
            VALUES ('{fecha}', {', '.join('NULL' if v is None else str(v) for v in valores.values())})
            ON DUPLICATE KEY UPDATE 
            {', '.join(f"{col} = VALUES({col})" for col in valores.keys())};
        """
        connection.execute(text(update_query))

print("✅ Datos insertados o actualizados correctamente en 'calculo_hueco_termico'.")

# 🔹 Verificar si la columna 'Suma_Generacion' existe antes de agregarla
query_verificar_columna = """
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'calculo_hueco_termico' 
    AND COLUMN_NAME = 'Suma_Generacion';
"""

with engine.begin() as connection:
    result = connection.execute(text(query_verificar_columna)).fetchall()
    if not result:  # Si la columna no existe, se agrega
        connection.execute(text("""
            ALTER TABLE calculo_hueco_termico 
            ADD COLUMN Suma_Generacion FLOAT DEFAULT NULL;
        """))
        print("➕ Columna 'Suma_Generacion' agregada en 'calculo_hueco_termico'.")

# 🔹 Actualizar los valores de la columna con la suma de los indicadores
with engine.begin() as connection:
    connection.execute(text("""
        UPDATE calculo_hueco_termico
        SET Suma_Generacion = COALESCE(Indicador_474, 0) 
                             + COALESCE(Indicador_1844, 0) 
                             + COALESCE(Indicador_1845, 0) 
                             + COALESCE(Indicador_1846, 0) 
                             + COALESCE(Indicador_1848, 0) 
                             + COALESCE(Indicador_1849, 0) 
                             + COALESCE(Indicador_1850, 0) 
                             + COALESCE(Indicador_541, 0) 
                             + COALESCE(Indicador_542, 0) 
                             + COALESCE(Indicador_543, 0);
    """))
    print("✅ Columna 'Suma_Generacion' actualizada correctamente.")
