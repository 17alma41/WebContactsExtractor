"""
Módulo principal para coordinar el proceso de scraping.
"""

import os
import time
import logging
import threading
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures

from src.core.config import MAX_WORKERS
from src.core.checkpoint_manager import CheckpointManager
from src.scraping.email_scraper import extract_emails_from_url
from src.scraping.social_scraper import extract_social_links_from_url
from src.utils.selenium_utils import setup_driver

# Configuración de logging
logger = logging.getLogger("scraper")

# Thread-local para los drivers
thread_local = threading.local()

def _init_thread_driver():
    """Inicializa un driver por hilo."""
    thread_local.driver = setup_driver()

def procesar_sitio(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Procesa un sitio web extrayendo emails y redes sociales.
    
    Args:
        row: Diccionario con datos de la fila, debe contener 'website'
        
    Returns:
        Diccionario con datos originales más resultados del scraping
    """
    raw = row.get('website', '')
    if pd.isna(raw) or not isinstance(raw, str):
        return {**row, 'email':'', 'facebook':'', 'instagram':'', 'linkedin':'', 'x':''}
    
    url = raw.strip()
    if not url.lower().startswith(('http://', 'https://')):
        return {**row, 'email':'', 'facebook':'', 'instagram':'', 'linkedin':'', 'x':''}
    
    # Usar el driver del thread local
    driver = thread_local.driver
    
    # Extraer emails
    emails = extract_emails_from_url(
        url,
        modo_verificacion='avanzado',
        driver=driver,
        wait_timeout=10
    )
    
    # Extraer redes sociales
    redes = extract_social_links_from_url(
        url,
        driver=driver,
        wait_timeout=10
    )
    
    # Combinar resultados
    return {
        **row,
        'email':      ', '.join(emails),
        'facebook':   ', '.join(redes.get('facebook', [])),
        'instagram':  ', '.join(redes.get('instagram', [])),
        'linkedin':   ', '.join(redes.get('linkedin', [])),
        'x':          ', '.join(redes.get('x', [])),
    }

def procesar_archivo_csv(
    archivo: str, 
    carpeta_entrada: str, 
    carpeta_salida: str,
    max_workers: int = 4,
    modo_prueba: bool = False,
    reanudar: bool = True
) -> bool:
    """
    Procesa un archivo CSV extrayendo información de sitios web.
    
    Args:
        archivo: Nombre del archivo CSV
        carpeta_entrada: Carpeta donde se encuentra el archivo
        carpeta_salida: Carpeta donde guardar el resultado
        max_workers: Número máximo de hilos para el scraping
        modo_prueba: Si se debe ejecutar en modo prueba (limitado)
        reanudar: Si se debe reanudar desde el último checkpoint
        
    Returns:
        True si el procesamiento fue exitoso, False en caso contrario
    """
    path_in = os.path.join(carpeta_entrada, archivo)
    path_out = os.path.join(carpeta_salida, archivo.replace('.csv', '.xlsx'))
    
    # Verificar si el archivo ya fue procesado o está vacío
    if os.path.exists(path_out):
        print(f"⏩ Archivo {archivo} ya procesado, saltando.")
        return True
    
    if os.path.getsize(path_in) == 0:
        print(f"⚠️ Archivo {archivo} vacío, saltando.")
        return True
    
    try:
        # Cargar datos
        df = pd.read_csv(path_in)
        if 'website' not in df.columns:
            print(f"⚠️ Archivo {archivo} no contiene columna 'website', saltando.")
            return False
        
        # Limitar filas en modo prueba
        if modo_prueba:
            df = df.head(20)
            print(f"🧪 Modo prueba: procesando solo 20 filas")
        
        # Convertir a lista de diccionarios
        rows = df.to_dict(orient='records')
        
        # Crear checkpoint manager
        checkpoint_manager = CheckpointManager(
            job_name=f"scrape_{archivo}"
        )
        
        print(f"\n▶️ Iniciando procesamiento de {archivo} con {len(rows)} filas")
        print(f"🧵 Utilizando {max_workers} hilos en paralelo")
        
        # Configurar checkpoint
        checkpoint_manager.set_total_rows(len(rows))
        
        # Si reanudar=True, filtrar elementos ya procesados
        if reanudar and not checkpoint_manager.is_completed():
            all_row_ids = list(range(len(rows)))
            pending_ids = checkpoint_manager.get_pending_rows(all_row_ids)
            
            if len(pending_ids) < len(rows):
                print(f"🔄 Reanudando desde checkpoint: {len(pending_ids)}/{len(rows)} elementos pendientes")
                # Filtrar solo elementos pendientes para procesar
                rows_to_process = [rows[i] for i in pending_ids]
            else:
                rows_to_process = rows
        else:
            rows_to_process = rows
        
        # Lista para almacenar resultados
        resultados = [None] * len(rows)
        
        # Función de procesamiento con gestión de errores y checkpoints
        def process_item_with_tracking(index_item):
            index, item = index_item
            url = item.get('website', '')
            
            # Verificar si ya está procesado en el checkpoint
            if checkpoint_manager.is_row_completed(index):
                print(f"⏩ Elemento {index}: ya procesado")
                return index, item
            
            try:
                # Procesar el elemento
                start_time = time.time()
                result = procesar_sitio(item)
                duration = time.time() - start_time
                
                # Registrar en checkpoint
                checkpoint_manager.mark_url_processed(
                    row_id=index,
                    url=url,
                    success=True,
                    result=result
                )
                
                print(f"✅ Procesado {url} en {duration:.2f}s")
                return index, result
            
            except Exception as e:
                # Registrar error
                error_msg = f"{type(e).__name__}: {str(e)}"
                print(f"❌ Error procesando {url}: {error_msg}")
                
                # Registrar en checkpoint
                checkpoint_manager.mark_url_processed(
                    row_id=index,
                    url=url,
                    success=False,
                    error=error_msg
                )
                
                # Devolver el elemento original con campos vacíos
                return index, item
        
        # Lista para almacenar drivers creados
        drivers = []
        
        try:
            # Procesar en paralelo
            with ThreadPoolExecutor(
                max_workers=max_workers,
                initializer=_init_thread_driver
            ) as executor:
                # Crear trabajos
                future_to_index = {
                    executor.submit(process_item_with_tracking, (i, item)): i 
                    for i, item in enumerate(rows_to_process)
                }
                
                # Procesar resultados a medida que se completan
                completed = 0
                total = len(future_to_index)
                
                for future in concurrent.futures.as_completed(future_to_index):
                    try:
                        index, result = future.result()
                        
                        # Almacenar resultado
                        if index < len(resultados):
                            resultados[index] = result
                        
                        # Actualizar progreso
                        completed += 1
                        if completed % 5 == 0 or completed == total:
                            print(f"📊 Progreso: {completed}/{total} ({completed/total*100:.1f}%)")
                            
                            # Guardar checkpoint periódicamente
                            checkpoint_manager.save()
                    
                    except Exception as e:
                        print(f"❌ Error inesperado en worker: {e}")
            
            # Marcar como completado si todo salió bien
            checkpoint_manager.mark_completed()
            
            # Construir DataFrame final
            df_res = pd.DataFrame(resultados)
            
            # Aplicar el orden de columnas definido en orden_columnas.txt
            from src.core.data_cleaner import limpiar_dataframe
            df_res, stats = limpiar_dataframe(df_res)
            print(f"🔄 Columnas reordenadas según configuración: {stats['columnas_reordenadas']}")
            
            # Importar el generador de Excel
            from src.core.excel.generator import generar_excel
            
            # Guardar Excel usando el generador
            generar_excel(df_res, nombre_archivo=path_out)
            
            print(f"✅ Procesamiento de {archivo} completado")
            return True
            
        finally:
            # Cerrar todos los drivers
            for driver in drivers:
                try:
                    driver.quit()
                except:
                    pass
    
    except Exception as e:
        logger.error(f"Error procesando archivo {archivo}: {e}")
        print(f"❌ Error procesando {archivo}: {e}")
        return False

def procesar_archivos_csv(
    carpeta_entrada: str, 
    carpeta_salida: str,
    max_workers: int = MAX_WORKERS,
    modo_prueba: bool = False
) -> List[str]:
    """
    Procesa todos los archivos CSV en una carpeta.
    
    Args:
        carpeta_entrada: Carpeta con archivos CSV a procesar
        carpeta_salida: Carpeta donde guardar los resultados
        max_workers: Número máximo de hilos para el scraping
        modo_prueba: Si se debe ejecutar en modo prueba (limitado)
        
    Returns:
        Lista de archivos procesados exitosamente
    """
    # Verificar que la carpeta existe
    if not os.path.isdir(carpeta_entrada):
        print(f"❌ No existe la carpeta {carpeta_entrada}")
        return []
    
    # Crear carpeta de salida si no existe
    os.makedirs(carpeta_salida, exist_ok=True)
    
    # Obtener lista de archivos CSV
    archivos = [f for f in os.listdir(carpeta_entrada) if f.lower().endswith('.csv')]
    
    if not archivos:
        print("⚠️ No se encontraron archivos CSV para procesar.")
        return []
    
    print(f"📋 Se procesarán {len(archivos)} archivos: {', '.join(archivos)}")
    
    # Preguntar si reanudar desde checkpoints
    reanudar = input("¿Reanudar desde el último punto guardado? (s/n): ").lower().startswith('s')
    
    # Lista para almacenar archivos procesados exitosamente
    archivos_procesados = []
    
    # Procesar cada archivo
    for i, nombre in enumerate(archivos, 1):
        print(f"\n[{i}/{len(archivos)}] Procesando: {nombre}")
        
        if procesar_archivo_csv(
            archivo=nombre,
            carpeta_entrada=carpeta_entrada,
            carpeta_salida=carpeta_salida,
            max_workers=max_workers,
            modo_prueba=modo_prueba,
            reanudar=reanudar
        ):
            archivos_procesados.append(nombre)
    
    return archivos_procesados
