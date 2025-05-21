"""
Módulo para generación de archivos Excel con datos y estadísticas.
"""

import os
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any

def generar_excel(df_resultado: pd.DataFrame, nombre_archivo: str, carpeta_salida: Optional[str] = None) -> str:
    """
    Genera un archivo Excel con los datos, estadísticas y metadatos.
    
    Args:
        df_resultado: DataFrame con los datos a guardar
        nombre_archivo: Nombre del archivo (sin extensión)
        carpeta_salida: Carpeta donde guardar el archivo (opcional)
        
    Returns:
        Ruta al archivo Excel generado
    """
    from src.core.config import OUTPUT_DIR
    
    # Determinar carpeta de salida
    if carpeta_salida is None:
        carpeta_salida = OUTPUT_DIR
    
    # Crear carpeta si no existe
    os.makedirs(carpeta_salida, exist_ok=True)
    
    # Determinar nombre de archivo
    if not nombre_archivo.lower().endswith('.xlsx'):
        nombre_archivo = nombre_archivo.replace('.csv', '') + '.xlsx'
    
    # Ruta completa al archivo Excel
    excel_path = os.path.join(carpeta_salida, nombre_archivo)
    
    # Calcular estadísticas
    num_domains = df_resultado.get("website", pd.Series()).astype(bool).sum()
    num_socials = 0
    
    for col in ['facebook', 'instagram', 'linkedin', 'x']:
        if col in df_resultado.columns:
            num_socials += df_resultado[col].astype(bool).sum()
    
    # Guardar Excel con múltiples hojas
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        # Hoja principal de datos
        df_resultado.to_excel(writer, sheet_name="data", index=False)
        
        # Configurar autofilter y asegurar que se mantiene el orden de columnas
        try:
            worksheet = writer.sheets["data"]
            last_col = len(df_resultado.columns)
            if last_col > 0:
                # Calcular la última columna (maneja más de 26 columnas correctamente)
                if last_col <= 26:
                    last_letter = chr(ord('A') + last_col - 1)
                else:
                    # Para más de 26 columnas (AA, AB, etc.)
                    first_char = chr(ord('A') + ((last_col - 1) // 26) - 1)
                    second_char = chr(ord('A') + ((last_col - 1) % 26))
                    last_letter = f"{first_char}{second_char}"
                
                # Aplicar autofilter a toda la tabla
                worksheet.auto_filter.ref = f"A1:{last_letter}{len(df_resultado)+1}"
                
                # Congelar la primera fila para facilitar navegación
                worksheet.freeze_panes = 'A2'
                
                print(f"✅ Autofilter aplicado a la hoja 'data'")
        except Exception as e:
            print(f"\n⚠️ Advertencia al configurar autofilter: {e}")
            # Continuar sin interrumpir el proceso

        # Hoja de estadísticas
        stats = {
            "Number of companies":       [len(df_resultado)],
            "Number of domains":         [num_domains],
            "Number of emails (valid)":  [df_resultado.get("email", pd.Series()).astype(bool).sum()],
            "Number of phone numbers":   [df_resultado.get("phone", pd.Series()).astype(bool).sum()],
            "Number of social networks": [num_socials],
        }
        df_stats = pd.DataFrame(stats)
        df_stats.to_excel(writer, sheet_name="statistics", index=False)

        # Sectores (main_category)
        if "main_category" in df_resultado.columns:
            df_sectors = (
                df_resultado["main_category"]
                .value_counts()
                .reset_index()
                .rename(columns={"index": "Sector", "main_category": "Number of companies"})
            )
            df_sectors.to_excel(writer, sheet_name="sectors", index=False)

        # Copyright
        copyright_text = (
            "Legal Notice\n"
            "    © companiesdata.cloud All rights reserved.\n"
            "    Registered with the Ministry of Culture and Historical Heritage GR-00416-2020.\n"
            "    https://companiesdata.cloud/ and https://www.centraldecomunicacion.es/\n"
            "\n"
            "    The data sources are the official websites of each company.\n"
            "    We do not handle personal data, therefore LOPD and GDPR do not apply.\n"
            "\n"
            "    The database is non-transferable and non-replicable.\n"
            "    Copying, distribution, or publication, in whole or in part, without express consent is prohibited.\n"
            "    Legal action will be taken for copyright infringements.\n"
            "\n"
            "    For more information, please refer to our FAQ:\n"
            "    https://companiesdata.cloud/faq and https://www.centraldecomunicacion.es/preguntas-frecuentes-bases-de-datos/\n"
            "\n"
            "    Reproduction, distribution, public communication, and transformation, in whole or in part,\n"
            "    of the contents of this database are prohibited without the express authorization of companiesdata.cloud and centraldecomunicacion.es\n"
            "    The data has been collected from public sources and complies with current regulations."
        )

        df_copyright = pd.DataFrame(
            [[line] for line in copyright_text.split("\n")]
        )
        df_copyright.to_excel(
            writer,
            sheet_name="copyright",
            index=False,
            header=False
        )

    print(f"📊 Excel generado con estadísticas y datos: {excel_path}")
    return excel_path
