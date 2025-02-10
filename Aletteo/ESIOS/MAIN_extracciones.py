# MAIN SCRIPT
# Desde aqui podras ejecutar todos los scripts para tener las extracciones necesarias entre las fechas indicadas

import subprocess
import os
from pathlib import Path

# 📌 Obtener la ruta relativa del directorio donde están los scripts de extracción
BASE_DIR = Path(__file__).resolve().parent  # Directorio donde está MAIN.py
script_dir = BASE_DIR / "Scripts_extracciones"  # Carpeta donde están los scripts

# 📌 Fechas definidas en el main script
start_date = "2025-01-01T00:00:00Z"
end_date = "2026-01-02T23:59:59Z"

# 📌 Lista de scripts a ejecutar (con ruta relativa)
scripts = [
    script_dir / "Extraer_Api_Produccion_Nuclear.py",
    script_dir / "Extraer_Api_Varios.py",
    script_dir / "Extraer_Api_Precios_Energia.py"  # Agrega más scripts aquí
]

# 📌 Ejecutar cada script con las fechas
for script in scripts:
    if script.exists():  # Verifica que el script exista antes de ejecutarlo
        print(f"\n🚀 Ejecutando {script} con fechas {start_date} a {end_date}...")
        try:
            subprocess.run(["python", str(script), start_date, end_date], check=True)
        except subprocess.CalledProcessError as e:
            print(f"❌ Error ejecutando {script}: {e}")
    else:
        print(f"⚠️ El script {script} no se encontró. Verifica su ubicación.")

print("\n✅ Todos los scripts han sido ejecutados.")
