
# 📬 WebContactsExtractor: Extracción Automática de Datos de Contacto

**WebContactsExtractor** es una herramienta modular y potente basada en Python para la extracción automática de direcciones de correo electrónico y perfiles de redes sociales desde sitios web listados en archivos `.csv`. Optimiza el proceso de recolección de información de contacto, ofreciendo funciones avanzadas de verificación, limpieza y generación de datos anonimizados listos para demostraciones.

---

## 🚀 Características Principales

-   **Extracción Automática de Emails**: Usa Selenium para una extracción robusta de direcciones de correo electrónico desde páginas web.
-   **Scraping de Redes Sociales**: Capaz de identificar y extraer enlaces a plataformas sociales comunes (Facebook, Instagram, LinkedIn, X/Twitter).
-   **Verificación Avanzada de Emails**: Incluye comprobaciones de formato, validez de dominio, registros MX, SPF, DKIM y verificación SMTP para garantizar la calidad de los datos.
-   **Exclusión Personalizada de Emails**: Permite filtrar correos según listas definidas por el usuario (palabras clave, nombres, indicadores de spam) ubicadas en `config/xclusiones_email/`.
-   **Anonimización de Datos (Generación de Demos)**: Genera versiones anonimizadas de tus datos para demostraciones o pruebas, protegiendo información sensible. Los archivos generados usan el sufijo `_demo` y se guardan en `data/demo_outputs/`.
-   **Gestión Flexible de Datos**:
    -   **Gestión de Columnas**: Herramientas para reordenar, renombrar o eliminar columnas de tus conjuntos de datos.
    -   **Salida en Excel**: Organiza los datos extraídos y procesados en archivos `.xlsx`.
-   **Captura de Imágenes**: Genera imágenes a partir de tablas estadísticas en archivos `.xlsx` durante ciertos procesos (como la exclusión de emails).
-   **Procesamiento Eficiente**:
    -   **Ejecución Paralela**: Utiliza `ThreadPoolExecutor` para procesamientos concurrentes, acelerando el rendimiento en grandes volúmenes de datos.
    -   **Limpieza Masiva de CSVs**: Funcionalidad para limpiar en bloque archivos CSV vacíos o irrelevantes.
-   **Estructura Lista para Producción**: Diseño modular y escalable para facilitar el mantenimiento y desarrollo futuro.
-   **Sistema de Puntos de Control**: Permite reanudar procesos interrumpidos, ahorrando tiempo y recursos.
-   **Caché Inteligente**: Usa un sistema de caché para almacenar resultados y evitar procesamiento redundante.

---

## 🗂 Estructura del Proyecto

```
WebContactsExtractor/
├── src/                           # Código fuente principal
│   ├── main.py                    # Script principal que ejecuta todo el flujo
│   ├── core/                      # Componentes núcleo (configuración, errores, Excel, etc.)
│   ├── scraping/                  # Módulos de web scraping
│   ├── exclusion/                 # Lógica de filtrado de emails
│   ├── masking/                   # Lógica de anonimización de datos (demos)
│   └── utils/                     # Scripts de utilidad (monitorización, limpieza)
├── config/                        # Archivos de configuración
│   ├── txt_config/                # Configuraciones generales en texto
│   └── xclusiones_email/          # Listas de exclusión de correos
├── data/                          # Entrada y salida de datos
│   ├── inputs/                    # Archivos CSV para el proceso de extracción
│   ├── outputs/                   # Resultados del proceso de extracción principal
│   ├── exclusion_outputs/         # Resultados del proceso de exclusión
│   ├── demo_inputs/               # Archivos para la generación de demos
│   └── demo_outputs/              # Archivos anonimizados (demo)
├── logs/                          # Archivos de registro (logs)
├── drivers/                       # Web drivers (p. ej., chromedriver.exe)
├── scripts/                       # Scripts utilitarios
│   ├── demo_masker.py             # Script para generar archivos demo
│   ├── main_xclusionEmail.py      # Script para el proceso de exclusión de correos
│   └── ficheros_datos.py          # Script para agregación de datos específica
├── requirements.txt               # Dependencias del proyecto
└── README.md                      # Este archivo
```

---

## ⚙️ Requisitos e Instalación

Para comenzar con WebContactsExtractor necesitas:

1.  **Python**: Versión 3.8 o superior.
2.  **Google Chrome**: Última versión estable instalada en tu sistema.
3.  **ChromeDriver**:
    *   Descarga la versión de ChromeDriver que coincida con tu versión de Google Chrome desde [ChromeDriver Official Site](https://chromedriver.chromium.org/downloads).
    *   Coloca el archivo `chromedriver.exe` (u otro para tu sistema operativo) en la carpeta `drivers/` del proyecto o asegúrate de que esté en tu variable de entorno `PATH`.

Luego, instala las dependencias de Python ejecutando en la raíz del proyecto:

```bash
pip install -r requirements.txt
```

---

## ▶️ Cómo Usar

### 1. Flujo Principal de Extracción de Datos

Este es el flujo principal para automatizar todo el proceso desde el scraping hasta la generación de archivos demo.

1.  Coloca tus archivos `.csv` (con URLs o nombres de empresas) en `data/inputs/`.
2.  Ejecuta el script principal desde la raíz del proyecto:
    ```bash
    python -m src.main
    ```
3.  El flujo completo incluye:
    *   **Extracción**: Scrapea sitios web para obtener correos y redes sociales → `data/outputs/`.
    *   **Exclusión**: Filtra correos no deseados según las listas en `config/xclusiones_email/` → `data/exclusion_outputs/`.
    *   **Generación de Demos**: Anonimiza los datos extraídos → `data/demo_outputs/` con sufijo `_demo`.

### 2. Generar Archivos Demo de Forma Independiente

Si necesitas crear archivos anonimizados desde datasets existentes:

1.  Coloca tus archivos `.csv` o `.xlsx` con datos sensibles en `data/demo_inputs/`.
2.  Ejecuta el script `demo_masker.py`:
    ```bash
    python scripts/demo_masker.py
    ```
3.  Los archivos anonimizados con sufijo `_demo` se generarán en `data/demo_outputs/`. Ejemplo de transformación:
    ```
    contacto@empresa.com  ->  c******@empresa.com
    +34 612 345 678       ->  +34 612 345 ***
    instagram.com/user    ->  instagram.com/****
    ```

### 3. Proceso de Exclusión de Emails (Independiente)

Para ejecutar sólo el filtrado de emails:

1.  Asegúrate de tener actualizadas las listas (`apellidos.txt`, `nombres.txt`, `spam.txt`) en `config/xclusiones_email/`.
2.  Coloca los archivos a procesar en `data/outputs/`.
3.  Ejecuta el script:
    ```bash
    python scripts/main_xclusionEmail.py
    ```
    El script filtrará los correos y guardará los resultados en `data/exclusion_outputs/`, además puede generar imágenes estadísticas.

### 4. Agregación de Datos Específica (`ficheros_datos.py`)

Este script recorre carpetas por país, localiza archivos de Excel, extrae métricas de la hoja "statistics", asocia imágenes `.jpg`, y genera un resumen en Excel.

1.  Configura `scripts/ficheros_datos.py` según tus rutas.
2.  Ejecuta el script:
    ```bash
    python scripts/ficheros_datos.py
    ```
3.  El resumen se guardará como archivo Excel (por defecto en `data/outputs/` o donde se configure).

---

## 📝 Notas

-   Asegúrate de configurar correctamente las rutas en los archivos y scripts.
-   Mantén actualizado tu ChromeDriver para evitar errores de compatibilidad.
-   Respeta siempre los términos de servicio de los sitios web. Usa delays y cambia user-agent si es necesario.

---

## 🤝 Contribuciones

¡Las contribuciones son bienvenidas! Si tienes sugerencias o mejoras, no dudes en abrir un *issue* o enviar un *pull request*.

---

## 📄 Licencia

Este proyecto está bajo la Licencia MIT.
