import requests
import os
import csv  # Para exportar a CSV
from dotenv import load_dotenv
from pathlib import Path

# Obtener la ruta del archivo token.env en la misma carpeta que este script
BASE_DIR = Path(__file__).resolve().parent
dotenv_path = BASE_DIR / "token.env"

# Cargar las variables desde el archivo token.env
load_dotenv(dotenv_path=dotenv_path)

# Obtener la API Key desde las variables de entorno
API_KEY = os.getenv("ESIOS_API_KEY")

if not API_KEY:
    raise ValueError("❌ ERROR: API Key no encontrada. Verifica el archivo 'token.env'.")

# URL del endpoint de indicadores
url = "https://api.esios.ree.es/indicators"

# Encabezados de la solicitud
headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": API_KEY  # Cargar la API Key desde el archivo env
}

# Realizar la solicitud GET
response = requests.get(url, headers=headers)

# Verificar si la solicitud fue exitosa
if response.status_code == 200:
    # Analizar la respuesta JSON
    data = response.json()
    indicators = data.get("indicators", [])

    print("📊 Listado de Indicadores Disponibles:")
    
    # Definir la ruta del archivo CSV
    csv_filename = BASE_DIR / "indicadores_esios.csv"

    # Crear y escribir en el archivo CSV
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["ID", "Nombre"])  # Encabezados del CSV

        # Guardar cada indicador
        for indicator in indicators:
            id_indicator = indicator.get('id', 'Sin ID')
            name_indicator = indicator.get('name', 'Sin Nombre')

            print(f"🆔 ID: {id_indicator} | 📖 Nombre: {name_indicator}")
            writer.writerow([id_indicator, name_indicator])  # Escribir en el CSV

    print(f"\n✅ Archivo CSV generado correctamente: {csv_filename}")

else:
    print(f"❌ Error en la solicitud: {response.status_code} - {response.text}")
