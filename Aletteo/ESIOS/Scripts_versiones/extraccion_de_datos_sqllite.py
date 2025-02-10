import requests
import pandas as pd
import os
import sqlite3  # 📌 Importamos SQLite para manejar la base de datos
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# Obtener la ruta del archivo token.env en la misma carpeta que este script
BASE_DIR = Path(__file__).resolve().parent
dotenv_path = BASE_DIR / "token.env"

# Cargar las variables desde el archivo token.env
load_dotenv(dotenv_path=dotenv_path)

# Obtener la API Key desde las variables de entorno
API_KEY = os.getenv("ESIOS_API_KEY")

if not API_KEY:
    raise ValueError("❌ ERROR: API Key no encontrada. Verifica el archivo 'token.env'.")

# Definir los IDs de los indicadores que queremos extraer
indicator_ids = [600, 1727]

# Definir el rango de fechas (FORMATO: "YYYY-MM-DDTHH:MM:SSZ")
start_date = "2025-01-01T00:00:00Z"  # Fecha de inicio
end_date = "2025-01-30T23:59:59Z"  # Fecha de fin

# URL base de la API de ESIOS
BASE_URL = "https://api.esios.ree.es/indicators"

# Configurar los encabezados de la solicitud
headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": API_KEY
}

# Diccionario para almacenar los datos de los indicadores
data_dict = {}

# Extraer datos de cada indicador
for indicator_id in indicator_ids:
    print(f"📥 Extrayendo datos del indicador {indicator_id} de {start_date} a {end_date}...")

    # Construir la URL con los parámetros de fecha
    url = f"{BASE_URL}/{indicator_id}?start_date={start_date}&end_date={end_date}"

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            # Extraer los valores del indicador
            values = data["indicator"].get("values", [])

            # Guardar los valores en el diccionario
            for entry in values:
                fecha = entry["datetime"]
                valor = entry["value"]

                if fecha not in data_dict:
                    data_dict[fecha] = {}

                data_dict[fecha][indicator_id] = valor  # Guardar el valor bajo el ID del indicador

        else:
            print(f"❌ ERROR {response.status_code} al obtener el indicador {indicator_id}: {response.text}")

    except Exception as e:
        print(f"❌ ERROR al obtener el indicador {indicator_id}: {e}")

# Crear un DataFrame con los datos extraídos
df = pd.DataFrame.from_dict(data_dict, orient="index")

# Renombrar las columnas con los nombres de los indicadores
df.columns = [f"Indicador_{col}" for col in df.columns]

# Resetear el índice para que la fecha sea una columna
df.index.name = "Fecha"
df.reset_index(inplace=True)

# Convertir la columna de fecha a formato datetime
df["Fecha"] = pd.to_datetime(df["Fecha"])

# 📌 **Guardar en la Carpeta "Import" en una subcarpeta con la fecha actual**
import_folder = BASE_DIR / "Import"
today_folder = import_folder / datetime.today().strftime("%Y-%m-%d")

# Crear las carpetas si no existen
today_folder.mkdir(parents=True, exist_ok=True)

# Definir la ruta del archivo CSV
csv_filename = today_folder / f"indicadores_{start_date[:10]}_to_{end_date[:10]}.csv"

# Guardar el CSV
df.to_csv(csv_filename, index=False, encoding="utf-8", sep=";")

print(f"\n✅ Datos guardados en {csv_filename}")

# 📌 **Ahora guardamos los datos en la base de datos SQLite**
# 🔴 **ACTUALIZA LA UBICACIÓN DE LA CARPETA "DATA" SEGÚN SU NUEVA RUTA**
db_folder = Path("C:/Users/Alejandro/Desktop/Proyecto/Aletteo/Data")  # 🚀 Nueva ruta de la carpeta Data
db_path = db_folder / "Base_datos.db"

# Crear la carpeta "Data" si no existe
db_folder.mkdir(parents=True, exist_ok=True)

# Conectar a la base de datos
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Crear la tabla si no existe
columns_sql = ", ".join([f'"{col}" REAL' for col in df.columns[1:]])  # Crear columnas dinámicamente
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS datos (
    Fecha TEXT PRIMARY KEY,
    {columns_sql}
);
"""
cursor.execute(create_table_sql)

# Insertar o actualizar los datos en la base de datos
for _, row in df.iterrows():
    fecha = row["Fecha"].strftime("%Y-%m-%d %H:%M:%S")  # Convertir la fecha a string
    valores = row.to_dict()
    valores.pop("Fecha")  # Quitamos la fecha del diccionario para insertar solo los valores
    
    # Generar nombres de columnas y valores dinámicos
    columns = ", ".join([f'"{col}"' for col in valores.keys()])
    placeholders = ", ".join(["?" for _ in valores])
    update_assignments = ", ".join([f'"{col}" = ?' for col in valores.keys()])

    # Intentar actualizar los datos existentes
    update_sql = f"""
    UPDATE datos
    SET {update_assignments}
    WHERE Fecha = ?;
    """
    cursor.execute(update_sql, list(valores.values()) + [fecha])

    # Si no se actualizó ninguna fila, insertamos una nueva
    if cursor.rowcount == 0:
        insert_sql = f"""
        INSERT INTO datos (Fecha, {columns})
        VALUES (?, {placeholders});
        """
        cursor.execute(insert_sql, [fecha] + list(valores.values()))  # ✅ Corrección aplicada aquí


# Guardar los cambios y cerrar la conexión
conn.commit()
conn.close()

print(f"\n✅ Datos guardados en la base de datos: {db_path}")
