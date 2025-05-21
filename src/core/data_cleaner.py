"""
Módulo para limpieza y transformación de datos basado en configuraciones.
"""

import os
import pandas as pd
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from src.core.config import CONFIG_DIR


def cargar_columnas_a_eliminar(ruta: Optional[str] = None) -> List[str]:
    """
    Carga la lista de columnas a eliminar desde un archivo de texto.
    
    Args:
        ruta: Ruta al archivo de configuración (opcional)
        
    Returns:
        Lista de nombres de columnas a eliminar
    """
    if ruta is None:
        ruta = os.path.join(CONFIG_DIR, "txt_config", "columnas_a_eliminar.txt")
    
    columnas = []
    try:
        with open(ruta, 'r', encoding='utf-8') as f:
            columnas = [line.strip() for line in f if line.strip()]
        print(f"📋 Cargadas {len(columnas)} columnas a eliminar")
    except Exception as e:
        print(f"⚠️ Error al cargar columnas a eliminar: {e}")
    
    return columnas


def cargar_orden_columnas(ruta: Optional[str] = None) -> List[str]:
    """
    Carga el orden deseado de columnas desde un archivo de texto.
    
    Args:
        ruta: Ruta al archivo de configuración (opcional)
        
    Returns:
        Lista de nombres de columnas en el orden deseado
    """
    if ruta is None:
        ruta = os.path.join(CONFIG_DIR, "txt_config", "orden_columnas.txt")
    
    columnas = []
    try:
        with open(ruta, 'r', encoding='utf-8') as f:
            columnas = [line.strip() for line in f if line.strip()]
        print(f"📋 Cargado orden para {len(columnas)} columnas")
    except Exception as e:
        print(f"⚠️ Error al cargar orden de columnas: {e}")
    
    return columnas


def cargar_renombrar_columnas(ruta: Optional[str] = None) -> Dict[str, str]:
    """
    Carga los mapeos para renombrar columnas desde un archivo de texto.
    
    Args:
        ruta: Ruta al archivo de configuración (opcional)
        
    Returns:
        Diccionario con mapeos {columna_actual: columna_nueva}
    """
    if ruta is None:
        ruta = os.path.join(CONFIG_DIR, "txt_config", "renombrar_columnas.txt")
    
    mapeos = {}
    try:
        with open(ruta, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and ':' in line:
                    actual, nueva = line.split(':', 1)
                    mapeos[actual.strip()] = nueva.strip()
        print(f"📋 Cargados {len(mapeos)} mapeos para renombrar columnas")
    except Exception as e:
        print(f"⚠️ Error al cargar mapeos de columnas: {e}")
    
    return mapeos


def limpiar_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Limpia un DataFrame aplicando las reglas de configuración:
    1. Eliminar columnas especificadas
    2. Renombrar columnas según mapeos
    3. Reordenar columnas según orden especificado
    
    Args:
        df: DataFrame a limpiar
        
    Returns:
        DataFrame limpio y diccionario con estadísticas de operaciones
    """
    stats = {
        "columnas_eliminadas": 0,
        "columnas_renombradas": 0,
        "columnas_reordenadas": 0
    }
    
    # Hacer una copia para no modificar el original
    df_limpio = df.copy()
    
    # 1. Eliminar columnas
    columnas_a_eliminar = cargar_columnas_a_eliminar()
    columnas_eliminadas = []
    
    for col in columnas_a_eliminar:
        if col in df_limpio.columns:
            df_limpio = df_limpio.drop(columns=[col])
            columnas_eliminadas.append(col)
        else:
            print(f"⚠️ Columna a eliminar '{col}' no encontrada en el DataFrame")
    
    stats["columnas_eliminadas"] = len(columnas_eliminadas)
    if columnas_eliminadas:
        print(f"🗑️ Columnas eliminadas: {', '.join(columnas_eliminadas)}")
    
    # 2. Renombrar columnas
    mapeos = cargar_renombrar_columnas()
    columnas_renombradas = {}
    
    for col_actual, col_nueva in mapeos.items():
        if col_actual in df_limpio.columns:
            df_limpio = df_limpio.rename(columns={col_actual: col_nueva})
            columnas_renombradas[col_actual] = col_nueva
        else:
            print(f"⚠️ Columna a renombrar '{col_actual}' no encontrada en el DataFrame")
    
    stats["columnas_renombradas"] = len(columnas_renombradas)
    if columnas_renombradas:
        print(f"✏️ Columnas renombradas: {columnas_renombradas}")
    
    # 3. Reordenar columnas
    orden_columnas = cargar_orden_columnas()
    columnas_ordenadas = []
    columnas_restantes = []
    
    # Filtrar solo las columnas que existen en el DataFrame
    for col in orden_columnas:
        if col in df_limpio.columns:
            columnas_ordenadas.append(col)
        else:
            print(f"⚠️ Columna para ordenar '{col}' no encontrada en el DataFrame")
    
    # Añadir columnas que no están en el orden especificado al final
    for col in df_limpio.columns:
        if col not in columnas_ordenadas:
            columnas_restantes.append(col)
    
    # Reordenar solo si hay columnas para ordenar
    if columnas_ordenadas:
        df_limpio = df_limpio[columnas_ordenadas + columnas_restantes]
        stats["columnas_reordenadas"] = len(columnas_ordenadas)
        print(f"🔄 Columnas reordenadas: {len(columnas_ordenadas)}")
        if columnas_restantes:
            print(f"ℹ️ Columnas adicionales (al final): {len(columnas_restantes)}")
    
    return df_limpio, stats


def limpiar_archivo_csv(ruta_entrada: str, ruta_salida: Optional[str] = None) -> str:
    """
    Limpia un archivo CSV aplicando las reglas de configuración.
    
    Args:
        ruta_entrada: Ruta al archivo CSV de entrada
        ruta_salida: Ruta donde guardar el archivo limpio (opcional)
        
    Returns:
        Ruta al archivo limpio
    """
    if ruta_salida is None:
        nombre_base = os.path.basename(ruta_entrada)
        nombre, ext = os.path.splitext(nombre_base)
        ruta_salida = os.path.join(os.path.dirname(ruta_entrada), f"{nombre}_clean{ext}")
    
    print(f"🔄 Limpiando archivo: {os.path.basename(ruta_entrada)}")
    
    try:
        # Cargar datos
        df = pd.read_csv(ruta_entrada)
        print(f"📊 Datos cargados: {len(df)} filas, {len(df.columns)} columnas")
        
        # Limpiar datos
        df_limpio, stats = limpiar_dataframe(df)
        
        # Guardar resultado
        os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
        df_limpio.to_csv(ruta_salida, index=False)
        
        print(f"✅ Archivo limpio guardado: {ruta_salida}")
        print(f"📊 Resultado: {len(df_limpio)} filas, {len(df_limpio.columns)} columnas")
        
        return ruta_salida
    
    except Exception as e:
        print(f"❌ Error al limpiar archivo CSV: {e}")
        return ruta_entrada


def limpiar_archivo_excel(ruta_entrada: str, ruta_salida: Optional[str] = None) -> str:
    """
    Limpia un archivo Excel aplicando las reglas de configuración.
    
    Args:
        ruta_entrada: Ruta al archivo Excel de entrada
        ruta_salida: Ruta donde guardar el archivo limpio (opcional)
        
    Returns:
        Ruta al archivo limpio
    """
    if ruta_salida is None:
        nombre_base = os.path.basename(ruta_entrada)
        nombre, ext = os.path.splitext(nombre_base)
        ruta_salida = os.path.join(os.path.dirname(ruta_entrada), f"{nombre}_clean{ext}")
    
    print(f"🔄 Limpiando archivo Excel: {os.path.basename(ruta_entrada)}")
    
    try:
        # Cargar todas las hojas
        xls = pd.ExcelFile(ruta_entrada)
        hojas = {}
        
        for sheet_name in xls.sheet_names:
            hojas[sheet_name] = pd.read_excel(ruta_entrada, sheet_name=sheet_name)
        
        print(f"📊 Hojas cargadas: {len(hojas)}")
        
        # Limpiar hoja principal de datos si existe
        if "data" in hojas:
            print("🔄 Limpiando hoja 'data'...")
            hojas["data"], stats = limpiar_dataframe(hojas["data"])
        
        # Guardar resultado
        os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
        with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
            for sheet_name, df in hojas.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✅ Archivo Excel limpio guardado: {ruta_salida}")
        
        return ruta_salida
    
    except Exception as e:
        print(f"❌ Error al limpiar archivo Excel: {e}")
        return ruta_entrada


def limpiar_archivos_en_carpeta(carpeta: str, carpeta_salida: Optional[str] = None) -> List[str]:
    """
    Limpia todos los archivos CSV y Excel en una carpeta.
    
    Args:
        carpeta: Ruta a la carpeta con archivos a limpiar
        carpeta_salida: Carpeta donde guardar los archivos limpios (opcional)
        
    Returns:
        Lista de rutas a los archivos limpios
    """
    if carpeta_salida is None:
        carpeta_salida = os.path.join(os.path.dirname(carpeta), f"{os.path.basename(carpeta)}_clean")
    
    os.makedirs(carpeta_salida, exist_ok=True)
    
    archivos = [f for f in os.listdir(carpeta) if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
    
    if not archivos:
        print(f"⚠️ No se encontraron archivos CSV o Excel en {carpeta}")
        return []
    
    print(f"📂 Procesando {len(archivos)} archivos...")
    
    archivos_limpios = []
    
    for archivo in archivos:
        ruta_entrada = os.path.join(carpeta, archivo)
        ruta_salida = os.path.join(carpeta_salida, archivo)
        
        if archivo.lower().endswith('.csv'):
            ruta_limpia = limpiar_archivo_csv(ruta_entrada, ruta_salida)
        else:
            ruta_limpia = limpiar_archivo_excel(ruta_entrada, ruta_salida)
        
        archivos_limpios.append(ruta_limpia)
    
    print(f"✅ Limpieza completada: {len(archivos_limpios)} archivos procesados")
    return archivos_limpios
