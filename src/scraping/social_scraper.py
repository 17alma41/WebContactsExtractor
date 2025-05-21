"""
Módulo para extracción de enlaces a redes sociales de sitios web.
"""

import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

from src.utils.selenium_utils import setup_driver
from src.core.error_handler import ErrorHandler

# Inicializar manejador de errores
error_handler = ErrorHandler()

@error_handler.with_retry(max_retries=3, delay=2.0)
def extract_social_links_from_url(
    url: str,
    driver=None,
    wait_timeout: int = 10
):
    """
    Extrae enlaces esenciales a redes sociales desde la URL dada.
    - url: dirección HTTP/HTTPS.
    - driver: instancia Selenium opcional (reutilizable).
    - wait_timeout: tiempo máximo a esperar por <a>.

    Retorna dict con claves 'facebook','instagram','linkedin','x' y listas de URLs.
    """
    if not url or not isinstance(url, str) or not url.lower().startswith(('http://', 'https://')):
        print(f"⚠️ URL inválida, saltando: {url}")
        return {}

    driver_created = False
    if driver is None:
        driver = setup_driver()
        driver_created = True

    try:
        print(f"\n🌐 Procesando URL: {url}")
        print("⏳ Cargando página...")
        driver.get(url)
        # Espera explícita a que al menos un enlace <a> esté presente
        WebDriverWait(driver, wait_timeout).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, 'a'))
        )
        # Opcional: desplazar hasta el final para cargar contenido dinámico
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        print("✅ Página cargada y enlaces listos.")

        links = driver.find_elements(By.TAG_NAME, 'a')
        print(f"🔍 {len(links)} enlaces encontrados. Filtrando redes sociales...")

        urls = [link.get_attribute('href') for link in links if link.get_attribute('href')]
        found = {"facebook": [], "instagram": [], "linkedin": [], "x": []}

        for u in urls:
            # Facebook: perfiles/páginas, no compartidos
            if "facebook.com/" in u and "sharer" not in u and "share" not in u and len(u) < 100:
                found["facebook"].append(u)
            # Instagram: perfiles, no compartir o stories
            elif "instagram.com/" in u and "share" not in u and "stories" not in u and len(u) < 100:
                found["instagram"].append(u)
            # LinkedIn: /in/ o /company/, no compartir
            elif (
                "linkedin.com/" in u and
                ("/in/" in u or "/company/" in u) and
                "share" not in u and
                "sharing" not in u and
                len(u) < 100
            ):
                found["linkedin"].append(u)
            # X / Twitter: perfiles, no compartir o intent
            elif (
                ("x.com/" in u or "twitter.com/" in u) and
                "share" not in u and
                "intent" not in u and
                len(u) < 100
            ):
                found["x"].append(u)

        # Eliminar duplicados
        for key in found:
            found[key] = list(set(found[key]))

        redes_encontradas = [k for k, v in found.items() if v]
        if redes_encontradas:
            print(f"🔗 Redes encontradas en {url}: {', '.join(redes_encontradas)}")
        else:
            print(f"ℹ️ No se encontraron redes sociales en {url}")

        return {k: v for k, v in found.items() if v}

    except TimeoutException:
        print(f"⏱️ Timeout al cargar {url}")
        return {}
    except Exception as e:
        print(f"❌ Error al extraer redes sociales de {url}: {e}")
        # Registrar error para análisis posterior
        error_handler.log_error(e, {"url": url, "operation": "extract_social_links"})
        return {}
    finally:
        if driver_created:
            print("🧹 Cerrando navegador...")
            driver.quit()
