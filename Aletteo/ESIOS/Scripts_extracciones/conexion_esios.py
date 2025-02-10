import os   # Permite interactuar con el sistema operativo (leer variables de entorno, manipular archivos, rutas, etc.).
from dotenv import load_dotenv  # Carga variables desde un archivo .env
from esios import ESIOSClient  # Importa el cliente de conexión a la API de ESIOS
from pathlib import Path  # Permite manejar rutas de archivos de forma segura

def connect_esios():
    """
    Conecta a la API de ESIOS usando el token almacenado en el archivo .env.
    
    Retorna:
    - client: Objeto de conexión ESIOSClient.
    """

    print("🔍 Cargando el archivo de configuración 'token.env'...")

    # Obtener la ruta absoluta del archivo token.env en la misma carpeta que el script
    BASE_DIR = Path(__file__).resolve().parent  # Obtiene la carpeta donde está este script
    dotenv_path = BASE_DIR / "token.env"  # Construye la ruta completa del archivo .env

    # Cargar las variables del archivo .env
    if load_dotenv(dotenv_path=dotenv_path):
        print(f"✅ Archivo 'token.env' cargado correctamente desde: {dotenv_path}")
    else:
        print(f"⚠️ Advertencia: No se pudo cargar el archivo 'token.env' desde: {dotenv_path}. Verifica su ubicación.")

    # Obtener el token de la variable de entorno
    token = os.getenv('ESIOS_API_KEY')  # Extrae el valor de la variable ESIOS_API_KEY definida en token.env.

    if not token:  # Si el token no se carga correctamente, el programa mostrará un error.
        raise ValueError("❌ ERROR: Token no encontrado. Verifica el archivo 'token.env'.")

    print("🔑 Token encontrado correctamente.")

    # Configurar el token para la conexión
    os.environ['ESIOS_API_KEY'] = token

    print("🔗 Intentando conectar con la API de ESIOS...")

    try:
        # Crear el cliente de conexión
        client = ESIOSClient()  # Crea un objeto que permite interactuar con la API de ESIOS.
        print("✅ Conexión establecida con éxito.")

        # Hacer una solicitud de prueba a la API para validar la conexión
        test_endpoint = client.endpoint(name='indicators')
        print("📊 Conexión a la API verificada correctamente. Se puede acceder a los datos.")
        
        return client  # Retorna este objeto, listo para hacer consultas.

    except Exception as e:
        print(f"❌ ERROR: No se pudo conectar con la API de ESIOS. Detalles: {e}")
        raise e  # Relanza el error para que el script lo maneje

# Tras esto, puedes pedir datos en otros scripts usando el cliente de la siguiente forma:
# client = connect_esios()

if __name__ == "__main__":
    connect_esios()  # Llamamos a la función directamente si el archivo se ejecuta como script
