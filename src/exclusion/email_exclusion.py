"""
Módulo para exclusión de emails no deseados según criterios configurables.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.table import Table as mpl_table
from typing import Dict, List, Tuple, Set, Any, Optional
from pathlib import Path

from src.core.config import (
    EXCLUSION_CONFIG_DIR, DATA_SHEET, STATS_SHEET, IMAGE_SIZE
)

def cargar_exclusiones(carpeta: Optional[str] = None) -> Set[str]:
    """
    Carga palabras de exclusión desde archivos de texto.
    
    Args:
        carpeta: Ruta a la carpeta con archivos de exclusión
        
    Returns:
        Conjunto de palabras de exclusión
    """
    if carpeta is None:
        carpeta = EXCLUSION_CONFIG_DIR
        
    exclusiones = set()
    for fn in os.listdir(carpeta):
        if fn.endswith(".txt"):
            with open(os.path.join(carpeta, fn), encoding="utf-8") as f:
                exclusiones.update(line.strip().lower() for line in f if line.strip())
    return exclusiones

def filtrar_y_contar(df: pd.DataFrame, exclusiones: Set[str]) -> Tuple[pd.DataFrame, int, int]:
    """
    Filtra emails según criterios de exclusión y cuenta resultados.
    
    Args:
        df: DataFrame con datos a filtrar
        exclusiones: Conjunto de palabras para excluir emails
        
    Returns:
        Tuple con (DataFrame filtrado, total emails eliminados, total emails restantes)
    """
    # Convertir columna email a listas separadas por comas
    orig_listas = df["email"].fillna("").apply(
        lambda cell: [e.strip() for e in str(cell).replace(';', ',').split(',') if e.strip()]
    )
    orig_counts = orig_listas.apply(len)

    # Filtrar emails que contienen palabras excluidas
    filt_listas = orig_listas.apply(
        lambda lst: [e for e in lst if not any(tok in e.lower() for tok in exclusiones)]
    )
    filt_counts = filt_listas.apply(len)

    # Crear copia del DataFrame con emails filtrados
    df_filtrado = df.copy()
    df_filtrado["email"] = filt_listas.apply(lambda lst: ", ".join(lst) if lst else pd.NA)

    # Calcular estadísticas
    total_eliminadas = (orig_counts - filt_counts).sum()
    total_restantes = filt_counts.explode().dropna().nunique()
    
    return df_filtrado, total_eliminadas, total_restantes

def generar_estadisticas(df_data: pd.DataFrame, df_sectors: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Genera estadísticas para el DataFrame procesado.
    
    Args:
        df_data: DataFrame con datos principales
        df_sectors: DataFrame con información de sectores
        
    Returns:
        DataFrame con estadísticas generadas
    """
    stats = {
        "Number of companies": len(df_data),
        "Number of emails (unique)": df_data["email"].dropna().apply(
            lambda x: [e.strip() for e in str(x).split(",") if e.strip()]
        ).explode().nunique(),
        "Number of phone numbers": df_data["phone"].dropna().count() if "phone" in df_data.columns else 0,
        "Mobile phones": df_data["phone"].dropna().count() if "phone" in df_data.columns else 0,
        "Number of domains": df_data["website"].dropna().nunique() if "website" in df_data.columns else 0,
        "Number of social networks": df_data[["facebook", "instagram", "linkedin", "x"]].notna().sum().sum() 
            if all(col in df_data.columns for col in ["facebook", "instagram", "linkedin", "x"]) else 0
    }
    return pd.DataFrame([stats])

def guardar_tabla_como_imagen(df: pd.DataFrame, path_imagen: str, title: Optional[str] = None, 
                             columns: Optional[List[str]] = None) -> None:
    """
    Guarda una tabla como imagen para incluir en informes, con el mismo estilo
    que las imágenes de ejemplo.
    
    Args:
        df: DataFrame a visualizar
        path_imagen: Ruta donde guardar la imagen
        title: Título opcional para la imagen
        columns: Lista opcional de columnas a incluir
    """
    max_chars = 40
    max_columns = 5
    max_rows = 20

    # Filtrar columnas si es necesario
    if columns:
        df = df[columns].copy()
    if df.shape[1] > max_columns:
        df = df.iloc[:, :max_columns].copy()
    df = df.head(max_rows).copy()

    # Truncar textos largos
    df = df.copy().astype(str).apply(
        lambda col: col.map(lambda x: x[:max_chars] + "…" if len(x) > max_chars else x)
    )

    # Crear figura
    fig, ax = plt.subplots(figsize=(IMAGE_SIZE[0] / 100, IMAGE_SIZE[1] / 100))
    ax.axis("off")

    # Determinar si estamos mostrando sectores
    is_sectors = any("sector" in c.lower() for c in df.columns) and any(
        tok in c.lower() for tok in ("number", "count") for c in df.columns
    )

    # Definir anchos de columna
    col_widths = []
    for idx, col in enumerate(df.columns):
        col_lower = col.lower()
        if is_sectors:
            if idx == 0:
                col_widths.append(0.6)  # Sector más ancho
            else:
                col_widths.append(0.4)  # Número de empresas
        elif "review" in col_lower:
            col_widths.append(0.05)
        elif "rating" in col_lower:
            col_widths.append(0.08)
        elif any(keyword in col_lower for keyword in ["name", "categories", "main_category"]):
            max_len = df[col].map(len).max()
            if max_len < 15:
                col_widths.append(0.18)
            elif max_len < 30:
                col_widths.append(0.24)
            else:
                col_widths.append(0.30)
        elif any(keyword in col_lower for keyword in ["email", "website", "address"]):
            col_widths.append(0.3)
        else:
            col_widths.append(0.12)

    # Crear tabla usando pandas.plotting.table
    from pandas.plotting import table
    tbl = table(ax, df, loc="center", colWidths=col_widths)

    # Ajustar propiedades de la tabla
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1.2, 1.2)

    # Ajustar colores y estilos como en las imágenes de ejemplo
    for key, cell in tbl.get_celld().items():
        cell.set_edgecolor('#cccccc')
        cell.set_linewidth(0.5)
        if key[0] == 0:  # Encabezados
            cell.set_facecolor('#e6f2ff')
            cell.set_text_props(weight='bold')
        else:  # Filas de datos
            cell.set_facecolor('#ffffff')

    # Título
    if title:
        ax.set_title(title, fontweight="bold", fontsize=13, pad=15)

    # Guardar imagen
    plt.tight_layout()
    fig.savefig(path_imagen, dpi=100)
    plt.close(fig)

def procesar_archivo_exclusion(path_entrada: str, exclusiones: Set[str], 
                               path_salida: Optional[str] = None, modo_prueba: bool = False) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Procesa un archivo Excel aplicando exclusiones de email.
    
    Args:
        path_entrada: Ruta al archivo Excel de entrada
        exclusiones: Conjunto de palabras para excluir emails
        path_salida: Ruta opcional para guardar el resultado
        
    Returns:
        Tuple con (diccionario de hojas procesadas, DataFrame de estadísticas)
    """
    # Cargar todas las hojas
    hojas = {}
    xls = pd.ExcelFile(path_entrada)
    for sheet_name in xls.sheet_names:
        hojas[sheet_name] = pd.read_excel(path_entrada, sheet_name=sheet_name)

    # Procesar hoja de datos
    if DATA_SHEET not in hojas:
        raise ValueError(f"No se encontró la hoja '{DATA_SHEET}' en el archivo")
        
    df_data = hojas[DATA_SHEET]
    
    # Limitar filas en modo prueba
    if modo_prueba:
        print(f"🧪 Modo prueba: procesando solo 20 filas de exclusión")
        # Crear una copia explícita para evitar SettingWithCopyWarning
        df_data = df_data.head(20).copy()
        
        # Asegurarse de que haya datos en todas las columnas necesarias para generar estadísticas
        if 'email' not in df_data.columns:
            df_data.loc[:, 'email'] = ''
        if 'main_category' not in df_data.columns:
            df_data.loc[:, 'main_category'] = 'Categoría de prueba'
        
    df_limpia, tot_elim, tot_rest = filtrar_y_contar(df_data, exclusiones)

    # Crear diccionario de salida
    hojas_out = {}
    hojas_out[DATA_SHEET] = df_limpia

    # Copiar hojas adicionales
    for name, df in hojas.items():
        if name not in (DATA_SHEET, STATS_SHEET):
            hojas_out[name] = df.copy()

    # Generar estadísticas
    df_stats = generar_estadisticas(df_limpia, hojas.get("sectors", pd.DataFrame(columns=["Sector"])))
    hojas_out[STATS_SHEET] = df_stats

    return hojas_out, df_stats

def guardar_hojas(hojas_dict: Dict[str, pd.DataFrame], path_salida: str) -> None:
    """
    Guarda un diccionario de hojas en un archivo Excel.
    
    Args:
        hojas_dict: Diccionario con nombres de hojas y DataFrames
        path_salida: Ruta donde guardar el archivo Excel
    """
    os.makedirs(os.path.dirname(path_salida), exist_ok=True)
    with pd.ExcelWriter(path_salida, engine="openpyxl") as writer:
        # Primero guardar hojas principales en orden específico
        for nombre in (DATA_SHEET, STATS_SHEET):
            if nombre in hojas_dict:
                hojas_dict[nombre].to_excel(writer, sheet_name=nombre, index=False)
        
        # Luego guardar el resto de hojas
        for nombre, df in hojas_dict.items():
            if nombre not in (DATA_SHEET, STATS_SHEET):
                df.to_excel(writer, sheet_name=nombre, index=False)
