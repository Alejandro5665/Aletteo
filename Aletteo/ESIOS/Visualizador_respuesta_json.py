import requests
import os
import json
from dotenv import load_dotenv
from pathlib import Path

# 📌 **Cargar configuración y credenciales**
BASE_DIR = Path(__file__).resolve().parent
dotenv_path = BASE_DIR / "Scripts_extracciones" / "token.env"
load_dotenv(dotenv_path=dotenv_path)

# 📌 **API Configuración**
API_KEY = os.getenv("ESIOS_API_KEY")
if not API_KEY:
    raise ValueError("❌ ERROR: API Key no encontrada. Verifica el archivo 'token.env'.")

BASE_URL = "https://api.esios.ree.es/indicators"

# 📌 **Indicador a consultar**
indicator_id = 1844  # Cambia este número por el ID del indicador que quieras analizar
start_date = "2025-01-01T00:00:00Z"
end_date = "2025-01-01T23:59:59Z"

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": API_KEY
}

# 📌 **Hacer la petición a la API**
url = f"{BASE_URL}/{indicator_id}?start_date={start_date}&end_date={end_date}"
response = requests.get(url, headers=headers)

# 📌 **Mostrar respuesta ordenada en la terminal**
if response.status_code == 200:
    data = response.json()

    # 📌 **Extraer y ordenar los valores**
    values = data["indicator"].get("values", [])
    
    sorted_values = sorted(values, key=lambda x: x["datetime_utc"])  # Ordenar por fecha UTC

    # 📌 **Imprimir en formato JSON bonito**
    print("\n📊 Respuesta JSON Ordenada del Indicador:\n")
    print(json.dumps(sorted_values, indent=4, ensure_ascii=False))  # Mostrar con formato y caracteres especiales

else:
    print(f"❌ ERROR {response.status_code}: {response.text}")
