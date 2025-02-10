import sys
import requests
import pandas as pd
import os
import mysql.connector
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import pytz  # 📌 Para manejar zonas horarias

# 📌 **Obtener fechas desde los argumentos del script principal**
start_date = sys.argv[1]  # Fecha de inicio
end_date = sys.argv[2]  # Fecha de fin

# 📌 **Cargar configuración y credenciales**
BASE_DIR = Path(__file__).resolve().parent
dotenv_path = BASE_DIR / "token.env"
load_dotenv(dotenv_path=dotenv_path)

# 📌 **Credenciales de MySQL**
MYSQL_HOST = "localhost"
MYSQL_USER = "Alejandro"
MYSQL_PASSWORD = "1999alex111"
MYSQL_DATABASE = "base_datos_mysql"

# 📌 **Conectar a la base de datos**
mysql_conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)
cursor = mysql_conn.cursor()

# 📌 **API Configuración**
API_KEY = os.getenv("ESIOS_API_KEY")
if not API_KEY:
    raise ValueError("❌ ERROR: API Key no encontrada. Verifica el archivo 'token.env'.")

BASE_URL = "https://api.esios.ree.es/indicators"

# 📌 **Indicadores a extraer**
indicator_ids = [600, 612, 613, 614, 1727]  # Agrega más indicadores si es necesario

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": API_KEY
}

# 📌 **Diccionario para almacenar datos por indicador**
dataframes = {}

# 📌 **Extraer datos de cada indicador**
for indicator_id in indicator_ids:
    print(f"\n📥 Extrayendo datos del indicador {indicator_id} de {start_date} a {end_date}...")

    url = f"{BASE_URL}/{indicator_id}?start_date={start_date}&end_date={end_date}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        values = data["indicator"].get("values", [])

        extracted_data = []
        for entry in values:
            fecha_utc = entry["datetime_utc"]
            valor = entry["value"]
            geo_name = entry.get("geo_name", "Global")

            # 📌 **Filtrar solo los datos de España si el indicador tiene geo_name**
            if "geo_name" in entry and geo_name != "España":
                continue

            # 📌 **Convertir fecha UTC a la zona horaria de España (CET/CEST)**
            fecha_dt = datetime.strptime(fecha_utc, "%Y-%m-%dT%H:%M:%SZ")
            fecha_utc = pytz.utc.localize(fecha_dt)  # Convertimos a UTC
            fecha_esp = fecha_utc.astimezone(pytz.timezone("Europe/Madrid"))  # Convertimos a España

            extracted_data.append({"Fecha": fecha_esp, f"Indicador_{indicator_id}": valor})

        # 📌 **Crear DataFrame si hay datos**
        if extracted_data:
            df = pd.DataFrame(extracted_data)
            df["Fecha"] = df["Fecha"].dt.strftime("%Y-%m-%d %H:%M:%S")  # Formato adecuado para MySQL

            dataframes[indicator_id] = df

            print("\n📊 Vista previa de los datos extraídos:")
            print(df.head())

        else:
            print(f"⚠️ No hay datos válidos para el indicador {indicator_id}")

    else:
        print(f"❌ ERROR {response.status_code}: {response.text}")

# 📌 **Fusionar todos los DataFrames en una única tabla**
if dataframes:
    final_df = list(dataframes.values())[0]
    for df in list(dataframes.values())[1:]:
        final_df = final_df.merge(df, on="Fecha", how="outer")  # Outer merge para mantener todas las fechas

    final_df = final_df.sort_values(by="Fecha").reset_index(drop=True)

    # 📌 **Verificar valores NaN en el DataFrame antes de MySQL**
    print("\n🔍 Valores nulos en el DataFrame antes de MySQL:")
    print(final_df.isna().sum())

    # 📌 **Crear la tabla en MySQL si no existe**
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Extracciones_horarias (
        Fecha DATETIME PRIMARY KEY
    );
    """)

    # 📌 **Verificar columnas de la tabla en MySQL**
    cursor.execute("SHOW COLUMNS FROM Extracciones_horarias;")
    existing_columns = {col[0] for col in cursor.fetchall()}

    # 📌 **Añadir nuevas columnas si no existen**
    for column in final_df.columns[1:]:
        if column not in existing_columns:
            cursor.execute(f"ALTER TABLE Extracciones_horarias ADD COLUMN `{column}` FLOAT NULL;")
            print(f"➕ Columna agregada: {column}")

    # 📌 **Insertar o actualizar datos en MySQL**
    for _, row in final_df.iterrows():
        fecha = row["Fecha"]
        valores = {col: (None if pd.isna(val) else val) for col, val in row.to_dict().items() if col != "Fecha"}

        # 📌 **Insertar o actualizar en un solo paso**
        if valores:
            columns = ", ".join([f"`{col}`" for col in valores.keys()])
            placeholders = ", ".join(["%s"] * len(valores))
            update_assignments = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in valores.keys()])

            insert_sql = f"""
            INSERT INTO Extracciones_horarias (Fecha, {columns})
            VALUES (%s, {placeholders})
            ON DUPLICATE KEY UPDATE {update_assignments};
            """

            cursor.execute(insert_sql, [fecha] + list(valores.values()))

    # 📌 **Guardar cambios y cerrar conexión**
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()

    print("\n✅ Datos guardados en la base de datos MySQL en la tabla 'Extracciones_horarias'.")

else:
    print("⚠️ No se extrajeron datos válidos.")
