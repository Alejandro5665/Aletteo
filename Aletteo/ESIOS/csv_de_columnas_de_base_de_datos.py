# extraer colunmas y los datos de estas de la base de datos a un csv

from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
from datetime import datetime

# 🔹 Configuración de la conexión a MySQL
usuario = "Alejandro"
contraseña = "1999alex111"
host = "localhost"
base_datos = "base_datos_mysql"

# Crear la conexión a MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{contraseña}@{host}/{base_datos}")

# 🔹 Definir el rango de fechas para la extracción
start_date = "2023-01-01 00:00:00"  # 📌 Modifica esta fecha según necesites
end_date = "2025-12-31 23:59:59"  # 📌 Modifica esta fecha según necesites

print(f"📅 Extrayendo datos entre {start_date} y {end_date}")

# 🔹 Obtener las columnas de la tabla `Extracciones_horarias_tratadas`
query_columnas = "SHOW COLUMNS FROM Extracciones_horarias_tratadas;"
df_columnas = pd.read_sql(query_columnas, engine)

# Lista de nombres de columnas en la base de datos
columnas_disponibles = df_columnas["Field"].tolist()
print("📌 Columnas disponibles:", columnas_disponibles)

# 🔹 Seleccionar las columnas que deseas exportar
columnas_seleccionadas = ["Fecha", "Indicador_600", "Indicador_1727", "Indicador_510"]  # Modifica según necesidades

# Validar que las columnas seleccionadas existan en la tabla
columnas_validas = [col for col in columnas_seleccionadas if col in columnas_disponibles]

if not columnas_validas:
    print("⚠️ No se han seleccionado columnas válidas. Revisa los nombres.")
else:
    print(f"✅ Columnas seleccionadas: {columnas_validas}")

    # 🔹 Extraer datos con las columnas seleccionadas
    query_datos = f"""
        SELECT {', '.join(columnas_validas)} 
        FROM Extracciones_horarias_tratadas
        WHERE Fecha BETWEEN '{start_date}' AND '{end_date}';
    """
    df_datos = pd.read_sql(query_datos, engine)

    # 📌 **Configurar la carpeta de almacenamiento con ruta relativa**
    BASE_DIR = Path(__file__).resolve().parent  # 📌 Obtiene el directorio donde está el script
    import_folder = BASE_DIR / "Import"

    # Crear la carpeta Import si no existe
    import_folder.mkdir(parents=True, exist_ok=True)

    # Carpeta con la fecha de hoy
    today_folder = import_folder / datetime.today().strftime("%Y-%m-%d")

    # Crear la carpeta del día si no existe
    today_folder.mkdir(parents=True, exist_ok=True)

    # Nombre del archivo CSV basado en el rango de fechas
    csv_filename = today_folder / f"indicadores_{start_date[:10]}_to_{end_date[:10]}.csv"

    # Guardar en CSV con separador ";"
    df_datos.to_csv(csv_filename, index=False, encoding="utf-8", sep=";")

    print(f"\n✅ Datos guardados en {csv_filename}")
