"""
Módulo para limpieza y preparación de archivos CSV.
"""

import os
import pandas as pd
from pathlib import Path
from typing import List, Optional

def limpiar_csvs_en_carpeta(carpeta_inputs: str, carpeta_outputs: str) -> List[str]:
    """
    Limpia y prepara archivos CSV de una carpeta.
    
    Args:
        carpeta_inputs: Ruta a la carpeta con archivos CSV originales
        carpeta_outputs: Ruta donde guardar los archivos CSV limpios
        
    Returns:
        Lista de archivos procesados
    """
    # Crear carpeta de salida si no existe
    os.makedirs(carpeta_outputs, exist_ok=True)
    
    # Lista para almacenar archivos procesados
    archivos_procesados = []
    
    # Procesar cada archivo CSV en la carpeta
    for filename in os.listdir(carpeta_inputs):
        if not filename.lower().endswith('.csv'):
            continue
            
        input_path = os.path.join(carpeta_inputs, filename)
        output_path = os.path.join(carpeta_outputs, filename)
        
        # Verificar si el archivo ya existe en la carpeta de salida
        if os.path.exists(output_path):
            print(f"⏩ Archivo ya procesado, saltando: {filename}")
            archivos_procesados.append(filename)
            continue
            
        try:
            # Leer CSV
            df = pd.read_csv(input_path, encoding='utf-8', on_bad_lines='skip')
            
            # Limpiar nombres de columnas
            df.columns = [col.strip().lower() for col in df.columns]
            
            # Normalizar nombres de columnas comunes
            column_mapping = {
                'url': 'website',
                'web': 'website',
                'sitio web': 'website',
                'sitio': 'website',
                'pagina web': 'website',
                'página web': 'website',
                'correo': 'email',
                'correo electrónico': 'email',
                'correo electronico': 'email',
                'e-mail': 'email',
                'mail': 'email',
                'teléfono': 'phone',
                'telefono': 'phone',
                'tel': 'phone',
                'tel.': 'phone',
                'móvil': 'phone',
                'movil': 'phone',
                'celular': 'phone',
                'nombre': 'name',
                'nombre empresa': 'name',
                'empresa': 'name',
                'compañía': 'name',
                'compania': 'name',
                'dirección': 'address',
                'direccion': 'address',
                'dir': 'address',
                'dir.': 'address',
                'ubicación': 'address',
                'ubicacion': 'address',
                'sector': 'sector',
                'categoría': 'sector',
                'categoria': 'sector',
                'industria': 'sector',
                'tipo': 'sector'
            }
            
            # Aplicar mapeo de columnas
            df = df.rename(columns={col: new_col for col, new_col in column_mapping.items() 
                                   if col in df.columns})
            
            # Asegurar que exista la columna 'website'
            if 'website' not in df.columns:
                print(f"⚠️ Columna 'website' no encontrada en {filename}, creando columna vacía")
                df['website'] = ''
                
            # Normalizar URLs
            if 'website' in df.columns:
                df['website'] = df['website'].apply(lambda x: normalizar_url(x) if pd.notna(x) else x)
                
            # Eliminar filas sin website o con website inválido
            df = df[df['website'].notna() & (df['website'] != '')]
            
            # Eliminar duplicados
            df = df.drop_duplicates(subset=['website'])
            
            # Guardar CSV limpio
            df.to_csv(output_path, index=False, encoding='utf-8')
            
            print(f"✅ Procesado: {filename} → {len(df)} filas")
            archivos_procesados.append(filename)
            
        except Exception as e:
            print(f"❌ Error procesando {filename}: {e}")
    
    return archivos_procesados

def normalizar_url(url: str) -> str:
    """
    Normaliza una URL para asegurar formato correcto.
    
    Args:
        url: URL a normalizar
        
    Returns:
        URL normalizada
    """
    if not isinstance(url, str):
        return ''
        
    url = url.strip().lower()
    
    # Eliminar espacios y caracteres no deseados
    url = url.replace(' ', '')
    
    # Añadir protocolo si falta
    if url and not url.startswith(('http://', 'https://')):
        url = 'https://' + url
        
    return url
