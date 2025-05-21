"""
Módulo para edición y procesamiento de columnas en archivos CSV/Excel.
"""

import os
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List, Any

def procesar_csvs_en_carpeta(
    carpeta_outputs: str,
    nuevo_orden: Optional[List[str]] = None,
    renombrar_columnas: Optional[Dict[str, str]] = None
) -> List[str]:
    """
    Procesa todos los CSV en una carpeta aplicando reordenamiento y renombrado de columnas.
    
    Args:
        carpeta_outputs: Ruta a la carpeta con archivos CSV
        nuevo_orden: Lista opcional con el nuevo orden de columnas
        renombrar_columnas: Diccionario opcional para renombrar columnas
        
    Returns:
        Lista de archivos procesados
    """
    from src.core.config import RENOMBRAR_COLUMNAS, NUEVO_ORDEN
    
    # Usar valores por defecto si no se proporcionan
    if nuevo_orden is None:
        nuevo_orden = NUEVO_ORDEN
        
    if renombrar_columnas is None:
        renombrar_columnas = RENOMBRAR_COLUMNAS
    
    # Verificar que la carpeta existe
    if not os.path.isdir(carpeta_outputs):
        print(f"❌ No existe la carpeta {carpeta_outputs}")
        return []
    
    # Lista para almacenar archivos procesados
    archivos_procesados = []
    
    # Procesar cada archivo CSV
    for filename in os.listdir(carpeta_outputs):
        if not filename.lower().endswith('.csv'):
            continue
            
        filepath = os.path.join(carpeta_outputs, filename)
        
        try:
            # Leer CSV
            df = pd.read_csv(filepath)
            
            # Aplicar renombrado si se especifica
            if renombrar_columnas:
                # Filtrar solo las columnas que existen en el DataFrame
                cols_to_rename = {old: new for old, new in renombrar_columnas.items() if old in df.columns}
                if cols_to_rename:
                    df.rename(columns=cols_to_rename, inplace=True)
            
            # Aplicar reordenamiento si se especifica
            if nuevo_orden:
                # Filtrar solo las columnas que existen en el DataFrame
                cols_validas = [col for col in nuevo_orden if col in df.columns]
                
                # Añadir columnas que no están en el nuevo orden al final
                cols_restantes = [col for col in df.columns if col not in nuevo_orden]
                
                # Reordenar columnas
                if cols_validas:
                    df = df[cols_validas + cols_restantes]
            
            # Guardar CSV procesado
            df.to_csv(filepath, index=False)
            
            print(f"✅ Columnas procesadas: {filename}")
            archivos_procesados.append(filename)
            
        except Exception as e:
            print(f"❌ Error procesando {filename}: {e}")
    
    return archivos_procesados
