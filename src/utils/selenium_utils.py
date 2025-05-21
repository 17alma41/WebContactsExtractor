"""
Utilidades para Selenium y configuración de drivers.
"""

import platform
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from src.core.config import BASE_DIR

def setup_driver(
    headless: bool = True,
    disable_gpu: bool = True,
    no_sandbox: bool = True,
    user_agent: str = "Mozilla/5.0",
    chromedriver_path: str = None,
    page_load_timeout: int = 15,
    implicit_wait: int = 10,
):
    """
    Configura y devuelve un driver de Selenium Chrome reutilizable.

    Parámetros:
      - headless: Ejecutar sin GUI.
      - disable_gpu: Desactivar GPU rendering.
      - no_sandbox: Añadir "--no-sandbox" y "--disable-dev-shm-usage".
      - user_agent: Cadena de agente de usuario.
      - chromedriver_path: Ruta al ejecutable de ChromeDriver. Si no se pasa, se busca
        en <proyecto>/drivers según el sistema operativo.
      - page_load_timeout: Timeout en segundos para carga de página.
      - implicit_wait: Tiempo de espera implícita para operar con find_element.
    """
    # Determinar ruta por defecto si no se proporciona
    if not chromedriver_path:
        # Elegir ejecutable según SO (Windows vs Linux/macOS)
        driver_name = "chromedriver.exe" if platform.system().lower().startswith("win") else "chromedriver"
        chromedriver_path = BASE_DIR / "drivers" / driver_name
    chromedriver_path = Path(chromedriver_path)
    if not chromedriver_path.exists():
        raise FileNotFoundError(f"❌ No se encontró ChromeDriver en: {chromedriver_path}")

    # Configurar opciones de Chrome
    opts = Options()
    if headless:
        opts.add_argument("--headless")
    if disable_gpu:
        opts.add_argument("--disable-gpu")
    if no_sandbox:
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument(f"user-agent={user_agent}")

    # Iniciar servicio y driver
    service = Service(str(chromedriver_path))
    driver = webdriver.Chrome(service=service, options=opts)

    # Configurar timeouts y espera implícita
    driver.set_page_load_timeout(page_load_timeout)
    driver.implicitly_wait(implicit_wait)

    return driver
