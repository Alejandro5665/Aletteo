import requests
import pandas as pd
import os
import mysql.connector  # 📌 Importamos MySQL Connector
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

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

# 1849, 1850, 474, 600, 1727, 612, 613, 614, 460, 510, 511, 512, 513, 514, 515, 516, 517, 1844, 1845, 1846, 1848, 541, 542, 543
indicator_ids = [474]
start_date = "2025-01-01T00:00:00Z"
end_date = "2025-12-31T23:59:59Z"
BASE_URL = "https://api.esios.ree.es/indicators"

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": API_KEY
}

data_dict = {}

# 📌 **Extraer datos de cada indicador**
for indicator_id in indicator_ids:
    print(f"📥 Extrayendo datos del indicador {indicator_id} de {start_date} a {end_date}...")

    url = f"{BASE_URL}/{indicator_id}?start_date={start_date}&end_date={end_date}"
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            values = data["indicator"].get("values", [])

            for entry in values:
                fecha = entry["datetime"]
                valor = entry["value"]

                if fecha not in data_dict:
                    data_dict[fecha] = {}

                data_dict[fecha][indicator_id] = valor

        else:
            print(f"❌ ERROR {response.status_code} al obtener el indicador {indicator_id}: {response.text}")

    except Exception as e:
        print(f"❌ ERROR al obtener el indicador {indicator_id}: {e}")

# 📌 **Convertir a DataFrame**
df = pd.DataFrame.from_dict(data_dict, orient="index")
df.columns = [f"Indicador_{col}" for col in df.columns]
df.index.name = "Fecha"
df.reset_index(inplace=True)
df["Fecha"] = pd.to_datetime(df["Fecha"], utc=True)

# 📌 **Guardar en la Carpeta "Import" un csv**
# import_folder = BASE_DIR / "Import"
# today_folder = import_folder / datetime.today().strftime("%Y-%m-%d")
# today_folder.mkdir(parents=True, exist_ok=True)
# csv_filename = today_folder / f"indicadores_{start_date[:10]}_to_{end_date[:10]}.csv"
# df.to_csv(csv_filename, index=False, encoding="utf-8", sep=";")
# print(f"\n✅ Datos guardados en {csv_filename}")

# 📌 **Crear la tabla si no existe**
cursor.execute("""
CREATE TABLE IF NOT EXISTS Extracciones_horarias (
    Fecha DATETIME PRIMARY KEY
);
""")

# 📌 **Verificar columnas de la tabla que queremos en la base de datos**
cursor.execute("SHOW COLUMNS FROM Extracciones_horarias;")
existing_columns = {col[0] for col in cursor.fetchall()}

# 📌 **Añadir nuevas columnas si no existen**
for column in df.columns[1:]:
    if column not in existing_columns:
        cursor.execute(f"ALTER TABLE Extracciones_horarias ADD COLUMN `{column}` FLOAT NULL;")
        print(f"➕ Columna agregada: {column}")

# 📌 **Insertar o actualizar datos**
for _, row in df.iterrows():
    fecha = row["Fecha"].strftime("%Y-%m-%d %H:%M:%S")
    valores = {col: (None if pd.isna(val) else val) for col, val in row.to_dict().items() if col != "Fecha"}

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
print("\n✅ Datos guardados en la base de datos MySQL en la tabla Extracciones_horarias.")
