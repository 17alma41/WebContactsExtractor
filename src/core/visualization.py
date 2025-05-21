"""
Módulo para funciones de visualización de datos.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from typing import Optional, List, Dict, Tuple


def crear_grafico_sectores(df: pd.DataFrame, 
                          columna_sector: str,
                          columna_conteo: str,
                          titulo: str = "Distribución por Sectores",
                          ruta_salida: str = None,
                          mostrar_top: int = 10) -> str:
    """
    Crea un gráfico de barras horizontal para visualizar la distribución de sectores.
    
    Args:
        df: DataFrame con los datos de sectores
        columna_sector: Nombre de la columna que contiene los sectores
        columna_conteo: Nombre de la columna que contiene el conteo de empresas
        titulo: Título del gráfico
        ruta_salida: Ruta donde guardar el gráfico. Si es None, se genera un nombre
        mostrar_top: Número de sectores a mostrar (los más frecuentes)
        
    Returns:
        str: Ruta donde se guardó el gráfico
    """
    # Ordenar y limitar a los top sectores
    df_sorted = df.sort_values(columna_conteo, ascending=False).head(mostrar_top)
    
    # Crear figura con tamaño adecuado
    plt.figure(figsize=(12, 8))
    
    # Crear gráfico de barras horizontal
    bars = plt.barh(df_sorted[columna_sector], df_sorted[columna_conteo], 
                    color='#3498db', alpha=0.8, height=0.6)
    
    # Añadir valores al final de cada barra
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 0.5, bar.get_y() + bar.get_height()/2, 
                 f'{int(width)}', ha='left', va='center', fontweight='bold')
    
    # Configurar estilo del gráfico
    plt.title(titulo, fontsize=16, pad=20, fontweight='bold')
    plt.xlabel('Número de Empresas', fontsize=12)
    plt.ylabel('Sector', fontsize=12)
    
    # Eliminar bordes
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['left'].set_color('#dddddd')
    plt.gca().spines['bottom'].set_color('#dddddd')
    
    # Añadir grid horizontal sutil
    plt.grid(axis='x', linestyle='--', alpha=0.3)
    
    # Ajustar diseño
    plt.tight_layout()
    
    # Guardar gráfico
    if ruta_salida is None:
        # Generar nombre basado en el título
        nombre_archivo = f"sector_chart_{titulo.lower().replace(' ', '_')}.jpg"
        directorio = os.path.join(os.getcwd(), 'data', 'exclusion_outputs')
        os.makedirs(directorio, exist_ok=True)
        ruta_salida = os.path.join(directorio, nombre_archivo)
    
    plt.savefig(ruta_salida, dpi=100, bbox_inches='tight')
    plt.close()
    
    return ruta_salida


def crear_grafico_estadisticas(estadisticas: pd.DataFrame,
                              titulo: str = "Statistics Overview",
                              ruta_salida: str = None) -> str:
    """
    Crea un gráfico de barras para visualizar estadísticas generales.
    
    Args:
        estadisticas: DataFrame con estadísticas (una sola fila)
        titulo: Título del gráfico
        ruta_salida: Ruta donde guardar el gráfico. Si es None, se genera un nombre
        
    Returns:
        str: Ruta donde se guardó el gráfico
    """
    # Transponer para que las categorías estén en el eje X
    stats_plot = estadisticas.T
    
    # Crear gráfico
    stats_plot.plot(kind="bar", legend=False, figsize=(12, 6), 
                   title=titulo, color="#3498db")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    
    # Guardar gráfico
    if ruta_salida is None:
        # Generar nombre basado en el título
        nombre_archivo = f"stats_chart_{titulo.lower().replace(' ', '_')}.jpg"
        directorio = os.path.join(os.getcwd(), 'data', 'exclusion_outputs')
        os.makedirs(directorio, exist_ok=True)
        ruta_salida = os.path.join(directorio, nombre_archivo)
    
    plt.savefig(ruta_salida, dpi=100)
    plt.close()
    
    return ruta_salida
