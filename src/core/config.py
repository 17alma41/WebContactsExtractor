"""
Configuración central del proyecto WebContactsExtractor.
Centraliza todas las rutas y parámetros de configuración.
"""

import os
from pathlib import Path

# Directorio base del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Directorios de datos
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "inputs"
CLEAN_INPUT_DIR = DATA_DIR / "clean_inputs"
OUTPUT_DIR = DATA_DIR / "outputs"
EXCLUSION_OUTPUT_DIR = DATA_DIR / "exclusion_outputs"
DEMO_OUTPUT_DIR = DATA_DIR / "demo_outputs"

# Directorios de configuración
CONFIG_DIR = BASE_DIR / "config"
TXT_CONFIG_DIR = CONFIG_DIR / "txt_config"
EXCLUSION_CONFIG_DIR = TXT_CONFIG_DIR / "xclusiones_email"

# Directorios de logs
LOG_DIR = BASE_DIR / "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Nombres de hojas Excel
DATA_SHEET = "data"
STATS_SHEET = "statistics"

# Parámetros de configuración
EMAIL_VERIFICATION_MODE = "avanzado"
MAX_WORKERS = 4  # Número de hilos para scraping
DEFAULT_TIMEOUT = 15  # Timeout por defecto para carga de páginas

# Parámetros de imágenes
IMAGE_SIZE = (1200, 630)

# Asegurar que existan los directorios necesarios
for directory in [
    INPUT_DIR, CLEAN_INPUT_DIR, OUTPUT_DIR, 
    EXCLUSION_OUTPUT_DIR, DEMO_OUTPUT_DIR, LOG_DIR
]:
    os.makedirs(directory, exist_ok=True)

def load_text_config(filename):
    """
    Carga configuración desde un archivo de texto.
    
    Args:
        filename: Nombre del archivo en TXT_CONFIG_DIR
        
    Returns:
        Lista de líneas no vacías del archivo
    """
    filepath = TXT_CONFIG_DIR / filename
    if not filepath.exists():
        return []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

# Cargar configuraciones comunes
COLUMNAS_A_ELIMINAR = load_text_config("columnas_eliminar.txt")

# Diccionario de renombrado: formato "antiguo:Nuevo"
RENOMBRAR_COLUMNAS = {}
for line in load_text_config("renombrar_columnas.txt"):
    if ":" in line:
        old, new = line.split(":", 1)
        RENOMBRAR_COLUMNAS[old.strip()] = new.strip()

# Orden de columnas
NUEVO_ORDEN = load_text_config("orden_columnas.txt")
