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

# 📌 **Indicador a extraer**
indicator_id = 474  
column_name = f"Indicador_{indicator_id}"  # Nombre de la columna en MySQL

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": API_KEY
}

# 📌 **Hacer la petición a la API**
print(f"\n📥 Extrayendo datos del indicador {indicator_id} de {start_date} a {end_date}...")
url = f"{BASE_URL}/{indicator_id}?start_date={start_date}&end_date={end_date}"
response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    values = data["indicator"].get("values", [])

    extracted_data = []
    for entry in values:
        fecha_utc = entry["datetime_utc"]  # Fecha en UTC
        valor = entry["value"]

        # 📌 **Convertir fecha UTC a la zona horaria de España (CET/CEST)**
        fecha_dt = datetime.strptime(fecha_utc, "%Y-%m-%dT%H:%M:%SZ")
        fecha_utc = pytz.utc.localize(fecha_dt)  # Convertimos a zona horaria UTC
        fecha_esp = fecha_utc.astimezone(pytz.timezone("Europe/Madrid"))  # Convertimos a España

        extracted_data.append({"Fecha": fecha_esp, column_name: valor})

    # 📌 **Convertir a DataFrame y sumar valores por fecha**
    df = pd.DataFrame(extracted_data)
    df["Fecha"] = df["Fecha"].dt.strftime("%Y-%m-%d %H:%M:%S")  # Formato adecuado para MySQL

    df_grouped = df.groupby("Fecha", as_index=False).sum()  # Agrupar por fecha sumando valores

    # 📌 **Mostrar vista previa**
    print("\n📊 Datos agrupados y sumados por fecha:")
    print(df_grouped.head())

    # 📌 **Crear la tabla en MySQL si no existe**
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Extracciones_horarias (
        Fecha DATETIME PRIMARY KEY
    );
    """)

    # 📌 **Verificar columnas existentes en la tabla**
    cursor.execute("SHOW COLUMNS FROM Extracciones_horarias;")
    existing_columns = {col[0] for col in cursor.fetchall()}

    # 📌 **Si la columna del indicador no existe, agregarla**
    if column_name not in existing_columns:
        cursor.execute(f"ALTER TABLE Extracciones_horarias ADD COLUMN `{column_name}` FLOAT NULL;")
        print(f"➕ Columna agregada: {column_name}")

    # 📌 **Insertar o actualizar datos en MySQL con `ON DUPLICATE KEY UPDATE`**
    for _, row in df_grouped.iterrows():
        fecha = row["Fecha"]
        valor = row[column_name]

        insert_sql = f"""
        INSERT INTO Extracciones_horarias (Fecha, `{column_name}`)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE `{column_name}` = VALUES(`{column_name}`);
        """
        cursor.execute(insert_sql, (fecha, valor))

    # 📌 **Guardar cambios y cerrar conexión**
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()

    print("\n✅ Datos guardados en la base de datos MySQL en la tabla 'Extracciones_horarias'.")

else:
    print(f"❌ ERROR {response.status_code}: {response.text}")
